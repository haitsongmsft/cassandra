package org.apache.cassandra.tools.cosmosdb;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.LivenessInfo;

public class ExpirationInfo {
	
	@SuppressWarnings("unused")
	private LivenessInfo liveInfo;
	
	public String tstamp;
	public int ttl;
	public String expires_at;
	public boolean expired = false;
	
	public ExpirationInfo(LivenessInfo liveInfo)
	{
		this.liveInfo = liveInfo;
        if (!liveInfo.isEmpty())
        {
        	this.tstamp = dateString(TimeUnit.MICROSECONDS, liveInfo.timestamp());
            if (liveInfo.isExpiring())
            {
                this.ttl = liveInfo.ttl(); 
                this.expires_at = dateString(TimeUnit.SECONDS, liveInfo.localExpirationTime());
                this.expired = liveInfo.localExpirationTime() < (System.currentTimeMillis() / 1000);
            }
        }		        
	}
	
    private String dateString(TimeUnit from, long time)
    {
        long secs = from.toSeconds(time);
        long offset = Math.floorMod(from.toNanos(time), 1000_000_000L); // nanos per sec
        return Instant.ofEpochSecond(secs, offset).toString();
    }
}
