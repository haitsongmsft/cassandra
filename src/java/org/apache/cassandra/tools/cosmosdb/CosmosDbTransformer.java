/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.tools.cosmosdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CosmosDbTransformer
{

    private static final Logger logger = LoggerFactory.getLogger(CosmosDbTransformer.class);
    
	private static ObjectMapper objMapper = new ObjectMapper();

    private final CFMetaData metadata;

    private final ISSTableScanner currentScanner;

    private boolean rawTime = false;

    private long currentPosition = 0;
    
    private CosmosIngester inj = new CosmosIngester();
    
    private CosmosDbTransformer(ISSTableScanner currentScanner, boolean rawTime, CFMetaData metadata)
    {
        this.metadata = metadata;
        this.currentScanner = currentScanner;
        this.rawTime = rawTime;
    }
    
    public static void IngestToCosmosDb(
    		ISSTableScanner currentScanner, 
    		Stream<UnfilteredRowIterator> partitions, 
    		boolean rawTime,
    		CFMetaData metadata,
    		String cqlCreateTable)
            throws IOException, InterruptedException
    {
        CosmosDbTransformer transformer = new CosmosDbTransformer(currentScanner, rawTime, metadata);
        transformer.inj.executeStatement(cqlCreateTable);
                
        ArrayList<Thread> threads = new ArrayList<Thread>();
        for(int i=0; i<10; i++) { threads.add(new Thread(transformer.inj)); }
        threads.forEach(t->t.start());
        
        partitions.forEach(part -> transformer.serializePartition(part));
        transformer.inj.addStatement("");
        
    	threads.forEach(t->{
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
    }

    private void updatePosition()
    {
        this.currentPosition = currentScanner.getCurrentPosition();
    }
            
    // returns a where clause for partitioning key;
    private ArrayList<String> serializePartitionKey(DecoratedKey key, ObjectNode rowNode)
    {    
    	ArrayList<String> result = new ArrayList<String>();
        AbstractType<?> keyValidator = metadata.getKeyValidator();
        try
        {                        
            if (keyValidator instanceof CompositeType)
            {
                // if a composite type, the partition has multiple keys.
                CompositeType compositeType = (CompositeType) keyValidator;
                ByteBuffer keyBytes = key.getKey().duplicate();
                // Skip static data if it exists.
                if (keyBytes.remaining() >= 2)
                {
                    int header = ByteBufferUtil.getShortLength(keyBytes, keyBytes.position());
                    if ((header & 0xFFFF) == 0xFFFF)
                    {
                        ByteBufferUtil.readShortLength(keyBytes);
                    }
                }

                List<ColumnDefinition> colDefs = metadata.partitionKeyColumns();
                int i = 0, iCol=0;
                while (keyBytes.remaining() > 0 && i < compositeType.getComponents().size())
                {
                    AbstractType<?> colType = compositeType.getComponents().get(i);

                    ByteBuffer value = ByteBufferUtil.readBytesWithShortLength(keyBytes);
                    String colVal = colType.getString(value);
                    String colJsonVal = colType.toJSONString(value, ProtocolVersion.CURRENT);

                    ColumnDefinition cdef = colDefs.get(iCol++);
                    String colName = cdef.name.toCQLString(); 
                    
                    result.add(colName);
                    result.add(colVal);
                    this.putJsonValue(colName, colJsonVal, rowNode);

                    byte b = keyBytes.get();
                    if (b != 0)
                    {
                        break;
                    }
                    ++i;
                }
            }
            else
            {
                // if not a composite type, assume a single column partition key.
                assert metadata.partitionKeyColumns().size() == 1;
            	ColumnDefinition cdef = metadata.partitionKeyColumns().get(0);
                String colName = cdef.name.toCQLString();
                ByteBuffer value = key.getKey();
            	String colVal = keyValidator.getString(value);
                String colJsonVal = cdef.cellValueType().toJSONString(value, ProtocolVersion.CURRENT);
                result.add(colName);
                result.add(colVal);
                this.putJsonValue(colName, colJsonVal, rowNode);
            }
        }
        catch (Exception e)
        {
            logger.error("Failure serializing partition key.", e);
        }
        return result;
    }

    private void serializePartition(UnfilteredRowIterator partition) 
    {
        try
        {           	
            String tableName = metadata.ksName + "." + metadata.cfName;
            ObjectNode partitionKeyNode = this.objMapper.createObjectNode();
            serializePartitionKey(partition.partitionKey(), partitionKeyNode);
            if (!partition.partitionLevelDeletion().isLive()) 
            {
                serializeDeletion(partition.partitionLevelDeletion());
                // to do, delete based on partition
                String partitionDelete = "delete from "+ tableName + " where " + this.assembleFields(partitionKeyNode, true);
                inj.addStatement(partitionDelete);
            }
            else 
            {
                // info only:
                System.out.println("Partition: table "+ this.assembleFields(partitionKeyNode, false));            	
            }

            if (partition.hasNext() || partition.staticRow() != null)
            {
                updatePosition();
                System.out.println("position: " + this.currentPosition);  
                
                if (!partition.staticRow().isEmpty())
                {
                    serializeRow(partitionKeyNode, partition.staticRow());
                }

                Unfiltered unfiltered;
                updatePosition();
                
                while (partition.hasNext())
                {
                    unfiltered = partition.next();
                    if (unfiltered instanceof Row)
                    {
                        serializeRow(partitionKeyNode, (Row) unfiltered);
                    }
                    else if (unfiltered instanceof RangeTombstoneMarker)
                    {
                        String rangeStr = serializeTombstone((RangeTombstoneMarker) unfiltered);
                        String rangeDelete= "delete from " + tableName + " where "+ 
                            this.assembleFields(partitionKeyNode, true) + " and " + rangeStr;
                        inj.addStatement(rangeDelete);
                    }
                    updatePosition();
                }
            }
        }
        catch (IOException e)
        {
            String key = metadata.getKeyValidator().getString(partition.partitionKey().getKey());
            logger.error("Fatal error parsing partition: {}", key, e);
        }
    }

    private void serializeRow(ObjectNode partitionKeyNode, Row row)
    {
        try
        {
        	
    		ObjectNode rowNode = objMapper.createObjectNode();
    		
    		// clone partition key info to row.
    		partitionKeyNode.getFields().forEachRemaining(fv->{ rowNode.put(fv.getKey(), fv.getValue()); });
        	
            LivenessInfo liveInfo = row.primaryKeyLivenessInfo();
            ExpirationInfo expInfo = new ExpirationInfo(liveInfo);
            
            boolean deleteOrExpired = !row.deletion().isLive() || expInfo.expired;
                		
            serializeClustering(row.clustering(), rowNode);
            
            for (ColumnData cd : row)
            {      
            	ColumnDefinition cdef = cd.column();
                String colName = cdef.name.toCQLString();
        		boolean isKey= cdef.isPrimaryKeyColumn() || cdef.isClusteringColumn() || cdef.isPrimaryKeyColumn();
        		if(deleteOrExpired && !isKey) 
        		{
        			// we don't care fields if the row is deleted, only keys are needed.
        			continue;
        		}
                String colVal = serializeColumnData(cd, liveInfo, rowNode);
        		// result.add(colName);
        		// result.add(colVal);
            }
            
            String tableName = metadata.ksName + "." + metadata.cfName;
                        
            if(!deleteOrExpired) 
            {
                String insertRowAsJson = "insert into " + tableName + " JSON '{" + this.assembleFields(rowNode, false) +"}'";
                logger.debug(insertRowAsJson);
                this.inj.addInsertRow("insert into "+ tableName, rowNode);
            }
            else 
            {
                String rowString = "delete from "+ tableName + " where "+ this.assembleFields(rowNode, true); 
                this.inj.addStatement(rowString);
            }
        }
        catch (IOException e)
        {
            logger.error("Fatal error parsing row.", e);
        }
    }

    private String serializeTombstone(RangeTombstoneMarker tombstone)
    	throws IOException
    {
        if (tombstone instanceof RangeTombstoneBoundMarker)
        {
            // range bound tomb stone bound:
            RangeTombstoneBoundMarker bm = (RangeTombstoneBoundMarker) tombstone;
            return serializeBound(bm.clustering(), bm.deletionTime(), "=");
        }
        else
        {
            assert tombstone instanceof RangeTombstoneBoundaryMarker;
            // range bound tomb stone bound: with boundary
            RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker) tombstone;
            String rangeStart = serializeBound(bm.openBound(false), bm.openDeletionTime(false), null);
            String rangeEnd = serializeBound(bm.closeBound(false), bm.closeDeletionTime(false), null);
            return rangeStart + " and " + rangeEnd;
        }
    }

    private String serializeBound(ClusteringBound bound, DeletionTime deletionTime, String operator) 
    		throws IOException
    {    	
    	if(operator == null) 
    	{
    		operator = bound.isStart() ? ">" : "<";
    		operator += bound.isInclusive() ? "=" : "";
    	}

    	StringBuilder sbFieldNames = new StringBuilder();
    	StringBuilder sbFieldValues = new StringBuilder();
        serializeClusteringForCQL(bound.clustering(), sbFieldNames, sbFieldValues);
        return sbFieldNames.toString() + operator + sbFieldValues.toString();
    }
        
    private void serializeClusteringForCQL(ClusteringPrefix clustering, StringBuilder fieldNames, StringBuilder fieldValues)
    {   
    	fieldNames.append("(");
    	fieldValues.append("(");
        List<ColumnDefinition> clusteringColumns = metadata.clusteringColumns();
        for (int i = 0; i < clusteringColumns.size() && i < clustering.size(); i++)
        {
        	String sep = i>0? ",": "";
        	fieldNames.append(sep);
        	fieldValues.append(sep);
            ColumnDefinition column = clusteringColumns.get(i);
    		String colName = column.name.toCQLString();
        	fieldNames.append(colName);
            String cellVal = column.cellValueType().toJSONString(clustering.get(i), ProtocolVersion.CURRENT);
        	fieldValues.append(cellVal);
        }
    	fieldNames.append(")");
    	fieldValues.append(")");
    }
    
    // serialize for json format clustering info via adding to rowNode.
    private ArrayList<String> serializeClustering(ClusteringPrefix clustering, ObjectNode rowNode) throws IOException
    {
    	ArrayList<String> result = new ArrayList<String>();
        List<ColumnDefinition> clusteringColumns = metadata.clusteringColumns();
        for (int i = 0; i < clusteringColumns.size() && i< clustering.size(); i++)
        {
            ColumnDefinition column = clusteringColumns.get(i);
            String colName = column.name.toCQLString();
        	String cellVal = column.cellValueType().toJSONString(clustering.get(i), ProtocolVersion.CURRENT);
        	result.add(colName);
        	result.add(cellVal);
        	this.putJsonValue(colName, cellVal, rowNode);
        }
        return result;
    }

	private String serializeDeletion(DeletionTime deletion) throws IOException
    {
    	return objMapper.writeValueAsString(new DeletionInfo(deletion));
    }

    private String serializeColumnData(ColumnData cd, LivenessInfo liveInfo, ObjectNode rowNode) 
    		throws JsonGenerationException, JsonMappingException, IOException
    {
    	ColumnDefinition cdef = cd.column();
    	
        String colName = cd.column().name.toCQLString();        
        if (cdef.isSimple())
        {
            return serializeCell((Cell) cd, liveInfo, rowNode);
        }
        else
        {
            ComplexColumnData complexData = (ComplexColumnData) cd;
            if (!complexData.complexDeletion().isLive())
            {
            	// as a delete, the delete info is not recorded in cosmosdb
                // return " null ";
            	rowNode.putNull(colName);
            	return " null "; // JSON format null, this will be used in insert statement cql string.
            }
            else 
            {
            	ObjectNode cellNode = rowNode.putObject(colName); // create a cell node to attach to row.
            	//StringBuilder sb = new StringBuilder();    	
            	//sb.append("{");
	            for (Cell subCell : complexData)
	            {
	            	String celName = subCell.column().name.toCQLString();
	                String celVal = serializeCell(subCell, liveInfo, cellNode);
	                //sb.append(sb.length()>1? "," : "");
	                //sb.append("\""+celName+"\" : "+ celVal);
	            }	            
            	// sb.append("}");
	            // return sb.toString();
	            String jsonValStr = objMapper.writerWithDefaultPrettyPrinter().writeValueAsString(cellNode);
	            return jsonValStr;
            }
       }            
    }
        
    // returns a json value of the cell, and assemble the rowNode with the cell's content.
    private String serializeCell(Cell cell, LivenessInfo liveInfo, ObjectNode rowNode) 
    		throws JsonGenerationException, JsonMappingException, IOException 
    {
        String cellValue = null;
    	ColumnDefinition cdef = cell.column();
        AbstractType<?> type = cdef.type;
        AbstractType<?> cellType = null;
        
        String colName = cdef.name.toCQLString();  
                    
        if (type.isCollection() && type.isMultiCell()) // non-frozen collection
        {
            CollectionType ct = (CollectionType) type;
            ArrayNode arrNode = objMapper.createArrayNode();
            for (int i = 0; i < cell.path().size(); i++)
            {
            	String cellArrItem = ct.nameComparator().getString(cell.path().get(i));
                arrNode.add(cellArrItem);
            }
            cellValue = objMapper.writerWithDefaultPrettyPrinter().writeValueAsString(arrNode);
            rowNode.put(colName, arrNode);   
            cellType = cell.column().cellValueType();
        }
        else if (type.isUDT() && type.isMultiCell()) // non-frozen udt
        {
            UserType ut = (UserType) type;
            ArrayNode arrNode = objMapper.createArrayNode();                
            for (int i = 0; i < cell.path().size(); i++)
            {
                Short fieldPosition = ut.nameComparator().compose(cell.path().get(i));                    
                String cellArrItem = (ut.fieldNameAsString(fieldPosition));
                arrNode.add(cellArrItem);
            }
            cellValue = objMapper.writerWithDefaultPrettyPrinter().writeValueAsString(arrNode);
            rowNode.put(colName, arrNode);                           
            Short fieldPosition = ((UserType) type).nameComparator().compose(cell.path().get(0));
            cellType = ((UserType) type).fieldType(fieldPosition);
        }
        else
        {
            cellType = cell.column().cellValueType();
        }
        if (cell.isTombstone())
        {
        	cellValue = " null "; // cql and json value of null
        	rowNode.putNull(colName);
        	//NOCOSMOSDBSUPPORT:
            //json.writeFieldName("deletion_info");
            //objectIndenter.setCompact(true);
            //json.writeStartObject();
            //json.writeFieldName("local_delete_time");
            //json.writeString(dateString(TimeUnit.SECONDS, cell.localDeletionTime()));
            //json.writeEndObject();
            //objectIndenter.setCompact(false);
        }
        else
        {
            // json.writeRawValue(cellType.toJSONString(cell.value(), ProtocolVersion.CURRENT));            	
        	cellValue = cellType.toJSONString(cell.value(), ProtocolVersion.CURRENT);
        	this.putJsonValue(colName, cellValue, rowNode); 
        }
        // NOT supported in cosmosdb: cell level info info if cell timestamp:
        if (liveInfo.isEmpty() || cell.timestamp() != liveInfo.timestamp())
        {
        	String timestamp = dateString(TimeUnit.MICROSECONDS, cell.timestamp());
        	//NOCOSMOSDBSUPPORT:: cellobjNode.put("tstamp", timestamp);
        	System.err.println("Time stamp for cell not supported: " +colName+" @" + timestamp );
        }
        if (cell.isExpiring() && (liveInfo.isEmpty() || cell.ttl() != liveInfo.ttl()))
        {
        	boolean expired = !cell.isLive((int) (System.currentTimeMillis() / 1000));
        	if(expired) 
        	{
        		cellValue= " null ";
        		rowNode.putNull(colName);
        	}
        	//NOCOSMOSDBSUPPORT:
        	String expiresAt = dateString(TimeUnit.SECONDS, cell.localDeletionTime());
        	System.err.println("TTL at cell not supported: " +colName + " ttl@" + cell.ttl() + " expires_at@" + expiresAt);
        }
        return cellValue;
    }
    
    private String dateString(TimeUnit from, long time)
    {
        if (rawTime)
        {
            return Long.toString(time);
        }
        
        long secs = from.toSeconds(time);
        long offset = Math.floorMod(from.toNanos(time), 1000_000_000L); // nanos per sec
        return Instant.ofEpochSecond(secs, offset).toString();
    }
    
    // method to assemble a field of json value format to the row node.
    private void putJsonValue(String fieldName, String jsonValue, ObjectNode rowNode) 
    		throws JsonProcessingException, IOException
    {
        JsonNode jsNode = objMapper.readTree("{\""+ fieldName +"\": "+jsonValue +"}");
        JsonNode jsValue = jsNode.get(fieldName);
        rowNode.put(fieldName, jsValue);
    }
    
    public static String assembleFields(ObjectNode rowNode, boolean asWhereClause)
    {
    	StringBuilder sb = new StringBuilder();
        char escape = asWhereClause? ' ': '"';
        char fieldEquals = asWhereClause? '=': ':';
        String splitter = asWhereClause? " and ": ",";
        rowNode.getFields().forEachRemaining(fv -> 
        {
        	String fieldName = fv.getKey();
        	JsonNode fieldValue = fv.getValue();
        	String fieldVal = getJsonString(fieldValue);
            String fieldSeperator = sb.length()==0? "": splitter;
            sb.append(fieldSeperator + escape + fieldName + escape + fieldEquals + fieldVal);
        });
        return sb.toString();
    }    
    
    public static String getJsonString(JsonNode jsnode)
    {
		try {
			return objMapper.writeValueAsString(jsnode);
		} catch (IOException e) {
			logger.error("FATAL ERROR");
			logger.error(e.getMessage());
			System.exit(1);
		}
		return null;
    }        
}