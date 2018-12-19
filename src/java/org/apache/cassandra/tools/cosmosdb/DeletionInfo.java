package org.apache.cassandra.tools.cosmosdb;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.DeletionTime;

public class DeletionInfo {

	private DeletionTime deletion;
	
	public String markedForDeleteAt;
	public String localDeletionTime;
	
	public DeletionInfo(DeletionTime deletion) {
		this.deletion = deletion;
		this.markedForDeleteAt = dateString(TimeUnit.MICROSECONDS, this.deletion.markedForDeleteAt());
		this.localDeletionTime = dateString(TimeUnit.SECONDS, this.deletion.localDeletionTime());
	}
	    
    private String dateString(TimeUnit from, long time)
    {
        long secs = from.toSeconds(time);
        long offset = Math.floorMod(from.toNanos(time), 1000_000_000L); // nanos per sec
        return Instant.ofEpochSecond(secs, offset).toString();
    }    
}
