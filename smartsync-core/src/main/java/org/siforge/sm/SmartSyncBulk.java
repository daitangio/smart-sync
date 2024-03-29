package org.siforge.sm;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

public class SmartSyncBulk {
	private Logger logger = Logger.getLogger(getClass());

	private DataSource source, destination;
	private List<String> relations2Sync = new LinkedList<String>();
	private int threads = 2;

	public int getRelations2SyncSize(){ return relations2Sync.size(); }
	public void setSource(DataSource source) {
		this.source = source;
	}

	public void setDestination(DataSource destination) {
		this.destination = destination;
	}

	public void setRelations2Sync(List<String> relations2Sync) {
		this.relations2Sync = relations2Sync;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public void addTables(String ...t){
		relations2Sync.addAll(Arrays.asList(t));
	}
	
	public void addTables(List<String> relations2Sync2) {
		this.relations2Sync.addAll(relations2Sync2);	
	}

	public void syncAll() throws SyncException {
		try {
			
			
			for (String table : relations2Sync) {
				SmartSync s = new SmartSync(table, source.getConnection(),
						destination.getConnection());
				try{
					logger.debug("Calling SmartSync on "+table);
					s.call();
				}catch(Exception e){
					logger.fatal("Failed sync on table:"+table);
					throw new SyncException(e);
				}
			}									
		} catch (Exception e) {
			throw new SyncException(e);
		}
	}

	/*
	 *
	 */	
	public void syncAllParallel() throws SyncException {
		try {
			List<ForkJoinTask<String>> tlist=new ArrayList<>();
			ForkJoinPool pool = new ForkJoinPool(threads,ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, false,
			0, threads*2, 1, null, 60_000L /*ForkJoinPool.DEFAULT_KEEPALIVE*/, TimeUnit.MILLISECONDS);
			logger.info("/- Submitting "+relations2Sync.size()+" Table to sync in parallel ForkJoinPool:"+threads);
			for (String table : relations2Sync) {
				SmartSync s = new SmartSync(table, source.getConnection(),
						destination.getConnection());
				tlist.add(pool.submit(s));
			}

			
			logger.info("Waiting task to finish...");			
			for (ForkJoinTask<String> forkJoinTask : tlist) {
				logger.info("Join for "+forkJoinTask.join());
			}
			
			

			pool.shutdown();
			while (!pool.awaitTermination(100, TimeUnit.MILLISECONDS)) {
				logger.trace("Waiting termination...Threads:"
						+ pool.getPoolSize() + " Task to end:"
						+ pool.getQueuedSubmissionCount());
			}
			logger.info("\\- Bulk Sync ok");
		} catch (SyncException | SQLException | InterruptedException e) {
			throw new SyncException(e);
		}
	}
	
}
