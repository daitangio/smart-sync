package org.siforge.sm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.apache.log4j.Category;
// import javax.servlet.http.*;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;


/** SmartSync sync the *content* of two tables between two JDBC connections.
 * SmartSync supports masking data. Scrambiling is on the go
 * @author Giovanni Giorgi
 */
public class SmartSync extends MetaSupport implements Callable<String>{

	/** Every how much time log processed data? */
	public static final int PERFROMANCE_LOG_SPAN_MS = 1200;
	Connection source,dest;
	int columns=0;
	String targetTable;
	String universalSelect, universalInsert;
	/**
	 * @param tableName
	 * @param srcCon
	 * @param destCon
	 * @param logz
	 * @throws SQLException
	 */    
	public SmartSync(String tableName, Connection srcCon, Connection destCon) throws SyncException {
		try {
			this.source=srcCon;
			this.dest=destCon;

			this.targetTable=tableName;
			
			NDC.push(targetTable);
			
			this.universalSelect="SELECT * FROM "+ this.targetTable;                

			ResultSet rs=source.createStatement().executeQuery(universalSelect);        

			if(!rs.next()) {
				logger.warn("Src Table is empty (?)");
			}

			ResultSetMetaData md=rs.getMetaData();
			columns=md.getColumnCount();


			// Ottiene i tipi sorgente:
			logger.info(this.targetTable+" <Source> COLUMNS: "+md.getColumnCount());
			this.srcColType=getTypes(md);



			// Fa lo stesso con la destinzione:


			ResultSet rdest=dest.createStatement().executeQuery(universalSelect);
			if(rdest.next()){
				logger.info("<Dest> Table isn't empty");
			}

			ResultSetMetaData mdDest=rdest.getMetaData();
			logger.info(this.targetTable+" <Dest>   COLUMNS: "+mdDest.getColumnCount());
			this.destColType=getTypes(mdDest);

			rs.close(); rdest.close();
			checkDestTypes();

			this.universalInsert="INSERT INTO "+this.targetTable +" VALUES (";
			int i=columns;
			while(i>1){
				this.universalInsert += "?,";
				i--;
			}
			this.universalInsert += " ?)";
			//log.debug(this.universalInsert);
		}catch(SQLException sqe){
			throw new SyncException(sqe);
		}finally {
			NDC.pop();
		}

	}

	java.util.Map<String,Object> column2suppress= new HashMap<>();

	/** Replace a column with a fixed one. i.e. destroy sensitive information
	 */
	public void mask(String column, Object maskedValue){
		column2suppress.put(column.toUpperCase(),maskedValue);
	}

	/** Metodo da chiamare per effettuare una sincronizzazione completa
	 */
	public String syncAll() throws SQLException {
		NDC.push(targetTable);
		logger.debug("SmartSync->"+targetTable);
		// Carico e scarico!
		// Ora inizia il ciclo:
		long startTime=System.currentTimeMillis();
		// MONO TRANSACTION:
		dest.setAutoCommit(false);
		source.setAutoCommit(false);
		// ? Push the lower level of isolation for the source
		// source.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

		int type, rowProcessed=0;
		Object objC;
		ResultSet rsSrc= this.source.createStatement().executeQuery(universalSelect);
		PreparedStatement insertStm=this.dest.prepareStatement(universalInsert);
		long cTime=System.currentTimeMillis();
		long masked_rows=0;
		while(rsSrc.next()){
			boolean masked=false;
			// Read from Src and write to dest....
			for(int i=1; i<= this.columns; i++){
				objC=rsSrc.getObject(i);
				type=findType(i);

				//log.debug("Processing COL:"+i+" Src:"+objC);
				if(objC!=null) {
					String columnName=rsSrc.getMetaData().getColumnName(i).toUpperCase();					
					if(column2suppress.size() > 0 && column2suppress.containsKey(columnName)){						
						insertStm.setObject(i, column2suppress.get(columnName), type);
						masked=true;
					}else {
						insertStm.setObject(i,objC, type);
					}
				}else{
					// Gli oggetti null vanno trattati diversamente.
					insertStm.setNull(i, type);
				}
			}			
			insertStm.execute();
			rowProcessed++;
			if(masked){
				masked_rows++;
			}
			if((System.currentTimeMillis()-cTime) > PERFROMANCE_LOG_SPAN_MS ) {
				cTime=System.currentTimeMillis();
				long timeTaken = System.currentTimeMillis()-startTime;
				logger.info("Records synced so far:"+rowProcessed+" Row/sec:"+                   
						(1000f*(rowProcessed)/timeTaken));
			}
		}
		dest.commit();
		rsSrc.close();
		final long timeTaken = System.currentTimeMillis()-startTime;
		final String result = "SmartSync->"+targetTable+" Rows Processed:"+rowProcessed+ (masked_rows>0?" *Masked* "+masked_rows:"") +" into "+timeTaken
				+ "ms Row/sec:"+        
				(1000f*(rowProcessed)/timeTaken);
		logger.info(result);
		NDC.pop();
		return result;
	}

	/** Check types and issue some warning
	 */
	void checkDestTypes() throws SyncException {
		//Confronta srcColType[], destColType[]
		// UNIMPLEMENTED
		if(!java.util.Arrays.equals(srcColType,destColType)){
			logger.debug("Tables Types di not match (on SQLite ignore this warning if dest table is empty)");
		}
	}

	/**
	 * @param args
	 */    
	public static void main(String[] args)  {



		Connection src=null, dest=null;
		Logger mlog=Logger.getLogger("dbaccess.SmartSyncMain.Main");

		try{
			mlog.debug("Getting Connections");
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			src  = DriverManager.getConnection("jdbc:mysql://localhost/dbapp?user=root&password=");
			dest = DriverManager.getConnection("jdbc:mysql://localhost/dbfru?user=root&password=");



			/** Esempio di processing di piu' tabelle.
			 * Vengono elencate in ordine di cancellazione, l'ordine 
			 * cioe' che consente di cancellare le tabelle senza incorrere
			 * in violazioni dei vincoli di integrita'
			 * L'ordine di popolamento e' esattamente l'opposto */
			String newsTables[]={"DOCUMENTI", "UTENTI"};
			processTables(newsTables,src,dest,mlog);                     
			dest.commit();

		}catch(Throwable e){
			mlog.error("Cannot proceed",e);
			try{
				src.rollback();
				dest.rollback();
			}catch(SQLException e2){
				mlog.error("Cannot Rollback: a very BAD DAY eh?",e2);
			};
		}
	}



	public static void processTables(String t[], Connection src, Connection dest,
			Logger log) throws SQLException {
		processTables(src, dest, log, false, t);
	}

	/** Process a bunch of tables.
	 * @param src
	 * @param dest
	 * @param log
	 * @param t
	 * @throws SQLException 
	 */
	public static void processTables(Connection src, Connection dest, Logger log,
			boolean deleteDest, String... t ) throws SQLException {
		// Prima esegue il big del:
		if(deleteDest){
			log.info("Requested deletation of target db....");
			for(int i=0; i<t.length; i++){
				log.info("Deleting "+t[i]);
				dest.createStatement().execute("DELETE FROM "+t[i]);
			}
		}        
		SmartSync sm;

		for(int i=t.length-1; i>=0; i--){
			sm=new SmartSync(t[i], src, dest);
			sm.syncAll();
		};
	}

	@Override
	public String call() throws Exception {		
		//logger.trace("Called by Concurrent Executor...");
		String r=syncAll();
		//logger.trace("Closing connections...");
		this.source.close();
		this.dest.close();
		return r;
	}
}
