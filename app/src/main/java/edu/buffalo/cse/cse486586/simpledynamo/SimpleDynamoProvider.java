package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {

	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";

	static final String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3,
			                             REMOTE_PORT4};
	static String[] hashedPorts = new String[5];

	static HashMap<String, String> hashPortMapping = new HashMap<String, String>();
	static String[] neighbours = new String[2];

	static final int SERVER_PORT = 10000;
	private static String myPort;
	private static String myHash;
	private static String myEmulatorPort;
	private static String v = "-v";
	private static String ACK = "ACK";
	private static int TIMEOUT = 100;

	// missed(while under faliure) insert HashMap  <portNum, "key,value">
	private static HashMap<String, List<String>> missedInsert = new HashMap<String, List<String>>();

	// missed(while under faliure) delete  <portNum, key>
    private static HashMap<String, List<String>> missedDelete = new HashMap<String, List<String>>();;

	private HashMap<String, Socket> sockets = new HashMap<String, Socket>();

	private HashMap<String, PrintWriter> socketsOut = new HashMap<String, PrintWriter>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		Context context = getContext();

		if(selection.equals("@")){
			deleteLocalFiles();
		}

		else if(selection.equals("*")){

			Callable<Object> deleteAll = new DeleteAll();
			ExecutorService executor = Executors.newFixedThreadPool(1);
			executor.submit(deleteAll);
			executor.shutdown();
		}

		else{

			Callable<Object> deleteTask = new DeleteTask(selection);
			ExecutorService executor = Executors.newFixedThreadPool(1);
			executor.submit(deleteTask);
			executor.shutdown();

		}

		return 1;
	}

	private void deleteLocalFiles(){

		Context context = getContext();
		String[] localFiles = context.fileList();
		for(String file:localFiles) {

			boolean deleted = context.deleteFile(file);
			if(!deleted)
				Log.e("deleteLocalFiles",file+" File Not Deleted");
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key = values.getAsString("key");
		String value = values.getAsString("value");
		String[] destinationPorts = getDestinationPortList(key);

		Callable<Object> insertTask = new InsertTask(key, value, destinationPorts);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		executor.submit(insertTask);

		return null;
	}

	public int forwardInsert(String key, String value, String destinationPort) {

		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(destinationPort));


			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

			// send request type
			out.println("insert");
			out.println(myEmulatorPort);

			// send key
			out.println(key);
			Log.d("forwardInsert","key "+key);

			//send value
			out.println(value);

			Log.d("insert", "request forwarded to "+destinationPort +
					 " with key = "+key+" value "+value);

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

//            DataInputStream in = new DataInputStream(socket.getInputStream());

            socket.setSoTimeout(TIMEOUT);

            String insertAck = in.readLine();

            if(insertAck == null){
                Log.e("forwardInsert","Socket Exception for key "+key+" on Port "+
                        destinationPort);
                return 0;
            }

            if(insertAck.equals(ACK)) {

                Log.d("insert", "ack received from " + destinationPort +
                        " for key = " + key);
                return 1;
            }
            else {
                Log.e("insert", "error: File not inserted on port "+destinationPort);
            }

		} catch (SocketTimeoutException e){
            Log.e("forwardInsert", e.getMessage());
            Log.e("insert", "error: File not inserted on port "+destinationPort);
        }catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return -1;
	}

	private void insertFile(String key, String value){

		Context context = getContext();

		try{
			FileOutputStream fos = context.openFileOutput(key, Context.MODE_PRIVATE);
			fos.write(value.getBytes());
			fos.close();
			Log.d("insert","filename "+key+" with value = "+value+ " inserted");
		}catch(Exception ex){
			Log.e("insert",ex.getMessage());
		}

	}

	private String getExistingVersion(String key){

		Context context = getContext();
		String[] fileList = context.fileList();
		//Log.d("getExistingVersion", Arrays.toString(fileList));

		for(String file:fileList){

			if(file.matches(key+"-v.*"))
				return file;
		}

		return null;

	}

	public void hashPorts(){

		try {
			int i = 0;
			for (String port : remotePorts) {

				// calculating the emulator port
				String remoteEmulatorPort = String.valueOf(Integer.parseInt(port) / 2);

				// getting hash value of emulator port
				String hashedPortValue = genHash(remoteEmulatorPort);
				hashedPorts[i] = hashedPortValue;
				i += 1;

				// storing the hashed port value and port
				hashPortMapping.put(hashedPortValue, port);
			}

			// sorting the hashed ports
			Arrays.sort(hashedPorts);


		}catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	public String[] getDestinationPortList(String key){

		String[] portList = new String[3];

		try {
			key = genHash(key);
		}catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		}

		String destPort = null;
		int destI = 0;

		for(int i=1; i<hashedPorts.length; i++){


			if(key.compareTo(hashedPorts[i-1]) > 0 && key.compareTo(hashedPorts[i]) < 0){
				destPort = hashPortMapping.get(hashedPorts[i]);
				destI = i;
				Log.d("getDestinationPortList","Normal Case");
				break;
			}

			// handling the keys which have the dest port = the first hashed port
			else if(i == hashedPorts.length-1 ){
				destPort = hashPortMapping.get(hashedPorts[0]);
				destI = 0;
				Log.d("getDestinationPortList","Other Case");
			}

		}

		portList[0] = destPort;

		for(int i=1; i<3; i++){
			destI+=1;

			int replicatePortId = (destI)%5;
			portList[i] =  hashPortMapping.get(hashedPorts[replicatePortId]);
		}

		return portList;

	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		try {

			TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			myPort = String.valueOf((Integer.parseInt(portStr) * 2));
			myEmulatorPort = String.valueOf((Integer.parseInt(portStr)));
			myHash = genHash(myEmulatorPort);
			hashPorts();
			System.out.println("myPort "+myPort);
			System.out.println("myEmulatorPort "+myEmulatorPort);

			// update on recovery
            ExecutorService updateExecutor = Executors.newSingleThreadExecutor();
            Callable<Object> updateTask = new UpdateTask();

            updateExecutor.submit(updateTask);


            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverSocket.setReuseAddress(true);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(new Server(serverSocket));
            executor.shutdown();

			// initializing the missedInsert and missedDelete HashMaps

            for(String port: remotePorts){

                if(port.equals(myPort))
                    continue;

                // initializing missed delete and insert lists for each port

                List<String> missedInsertPort = new ArrayList<String>();
                List<String> missedDeletePort = new ArrayList<String>();

                missedInsert.put(port, missedInsertPort);
                missedDelete.put(port, missedDeletePort);

            }



			}

		catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		MatrixCursor mc = null;


		if(selection.equals("@")){
			HashMap<String, String> files = readLocalFiles();
			mc = hashMapToCursor(files);
		}

		else if(selection.equals("*")){

			try {
				Callable<HashMap<String, String>> queryAll = new QueryAll();
				ExecutorService executor = Executors.newFixedThreadPool(1);
				Future<HashMap<String, String>> queryAllResult = executor.submit(queryAll);
				HashMap<String, String> allFiles = queryAllResult.get();

				mc = hashMapToCursor(allFiles);

			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		else{

			// locate the correct destination port to request
			// send the request to destination
			// read result

			try {

				String[] destinationPorts = getDestinationPortList(selection);
				String fileContent;


				Log.d("query", "key "+selection);

				Callable<String> queryTask = new QueryTask(selection, destinationPorts);
				ExecutorService executor = Executors.newFixedThreadPool(1);
				Future<String> queryResult = executor.submit(queryTask);



				fileContent = queryResult.get();
				String[] queryValues = {selection, fileContent};

				Log.d("query found ", Arrays.toString(queryValues));
				String[] columnNames = {"key", "value"};
				MatrixCursor mc1 = new MatrixCursor(columnNames);
				mc1.addRow(queryValues);

				return mc1;


			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}


		}

		return mc;
	}

	private String openFile(String key){

		String fileContent = null;

		try {

			Context context = getContext();

			FileInputStream fis = context.openFileInput(key);
			InputStreamReader is = new InputStreamReader(fis, StandardCharsets.UTF_8);

			BufferedReader reader = new BufferedReader(is);
			StringBuilder sb = new StringBuilder();
			String line = reader.readLine();

			while (line != null) {
				sb.append(line);
				line = reader.readLine();

			}

			fileContent = sb.toString();

		}catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fileContent;

	}

	private HashMap<String, String> readLocalFiles(){

		HashMap<String, String> files = new HashMap<String, String>();
		Context context = getContext();
		String[] localFiles = context.fileList();


		for(String key:localFiles) {

			String fileContent = openFile(key);

			String actualKey = key.split(v)[0];

			files.put(actualKey, fileContent);

		}

		return files;

	}

	private MatrixCursor hashMapToCursor(HashMap<String, String> files){

		String[] columnNames = {"key", "value"};
		MatrixCursor mc = new MatrixCursor(columnNames);

		for(Map.Entry<String, String> file: files.entrySet()){

			String[] queryValues = {file.getKey(), file.getValue()};
			mc.addRow(queryValues);

		}

		return mc;

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


	private class QueryTask implements Callable{


		String key;
		String[] destinationPorts;

		public QueryTask(String key, String[] destinationPorts){
			this.key = key;
			this.destinationPorts = destinationPorts;
		}


		@Override
		public String call() throws Exception {

			String fileContent = null;
			String coordinatorPort = destinationPorts[0];

			if(coordinatorPort.equals(myPort)) {
				fileContent = openFile(key);
			}

			else {

			    try {

                    for (String port : destinationPorts) {

                        Socket socket = new Socket();
                        SocketAddress address = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));

                        socket.connect(address, TIMEOUT);

                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                        out.println("read");
                        out.println(myEmulatorPort);

                        out.println(key);
                        Log.d("read", "request sent for key " + key + " to port " + port);

                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String response = in.readLine();

                        if(response == null || response.equals("null")){
                            continue;
                        }

                        fileContent = response;

                        Log.d("read", "response received for key " + key + " value: " +
                                fileContent + " from port " + port);


                    }
                }catch (SocketTimeoutException e){
			        Log.e("QueryTask", e.getMessage());
                }
            }

			return fileContent;
			}


		}

	private class InsertTask implements Callable{

		String key;
		String value;
		String[] destinationPorts;

		public InsertTask(String key, String value, String[] destinationPorts){

			this.key = key;
			this.value = value;
			this.destinationPorts = destinationPorts;
		}

		@Override
		public Object call() throws Exception {

		    Log.d("InsertTask", Arrays.toString(destinationPorts)+" Destination Ports for key "+key);


			for(String destinationPort: destinationPorts){

				//Log.d("insert", "my Port "+myPort);
				Log.d("insert", "key "+key+" destination port "+destinationPort);

				if(destinationPort.equals(myPort)) {
					Log.d("insert","key "+key+" inserted locally");
					insertFile(key, value);
				}
				else{
					int isInserted = forwardInsert(key, value, destinationPort);

					if(isInserted == 0){
					    // file not inserted
                        Log.d("insert","key "+key+" not inserted on port "+destinationPort);
					    missedInsert.get(destinationPort).add(key+","+value);
                    }

				}
			}

			return null;
		}
	}

	private class DeleteTask implements Callable{

		String key;

		public DeleteTask(String key){

			this.key = key;
		}

		@Override
		public Object call() throws Exception {

			Context context = getContext();
			try {
				String[] destinationPorts = getDestinationPortList(key);

				for(String port: destinationPorts){

					if(port.equals(myPort)) {
						context.deleteFile(key);
					}
					else {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port));
						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

						out.println("delete");
						out.println(myEmulatorPort);
						out.println(key);

						// reading ACK

                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String deleteAck = in.readLine();

                        if(deleteAck==null){
                            Log.e("DeleteTask", "Delete of "+key+" failed on port "+port);
                            missedDelete.get(port).add(key);
                        }

					}

				}

			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	private class DeleteAll implements Callable{


		@Override
		public Object call() throws Exception {

			try {

				deleteLocalFiles();

				for (String port : remotePorts) {

					if (port.equals(myPort))
						continue;

					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port));
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					out.println("deleteAll");
					out.println(myEmulatorPort);

				}
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			return null;
		}
	}

	private class QueryAll implements Callable<HashMap<String, String>>{


		@Override
		public HashMap<String, String> call() throws Exception {

			HashMap<String, String> allFiles = new HashMap<String, String>();

			try {
				HashMap<String, String> localFiles = readLocalFiles();
				allFiles.putAll(localFiles);

				for (String port : remotePorts) {

					if (port.equals(myPort))
						continue;

					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port));

					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					out.println("queryAll");
					out.println(myEmulatorPort);

					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));


					// get total number of files
					String response = in.readLine();

					if(response == null){
					    Log.d("QueryAll","Failed for port "+port);
					    continue;
                    }

					int numFiles = Integer.parseInt(response);

					// read the files, store in hashmap

					for(int i=0; i<numFiles; i++){

						// receive key
						String key = in.readLine();

						// receive value
						String value = in.readLine();

						allFiles.put(key, value);
					}

				}



			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			return allFiles;
		}
	}

	private class UpdateTask implements Callable{


        @Override
        public Object call() throws Exception {

            for(String port: remotePorts){

                if(port.equals(myPort))
                    continue;

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);


                out.println("update");
                out.println(myEmulatorPort);

                out.println(myPort);

                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // num of inserts
                String numInserts = in.readLine();

                for(int i = 0 ; i < Integer.parseInt(numInserts); i++){

                    String key = in.readLine();
                    String value = in.readLine();

                    insertFile(key, value);

                    Log.d("update", key+" updated with value "+value);
                }

                // num of delete
                String numDelete = in.readLine();

                for(int i = 0; i< Integer.parseInt(numDelete); i++){

                    String key = in.readLine();
                    boolean result = getContext().deleteFile(key);

                    if(result)
                        Log.d("update", key+" deleted");
                }


            }

            return null;
        }
    }

	private class Server implements Runnable{

		ServerSocket serverSocket;

		public Server(ServerSocket serverSocket){

			this.serverSocket = serverSocket;
		}


		@Override
		public void run() {

			while(true){

                String request;
				try {
					Log.d("Server", "Server Running");

					Socket clientSocket;

					synchronized (serverSocket) {
					clientSocket = serverSocket.accept();
					}

					BufferedReader in = new BufferedReader(new InputStreamReader(
							clientSocket.getInputStream()));

					Log.d("Server", "Waiting for request");

					request = in.readLine();

					if(request == null){

						Log.e("Server", "request null");
						//Thread.sleep(200);
						//continue;
					}

					String requestingPort = in.readLine();
					Log.d("Server", "Request Received: "+request+" from "+requestingPort);

					if(request.equals("insert")){

						String key = in.readLine();
						Log.d("Server Insert", "key "+key);

						String value = in.readLine();

						insertFile(key, value);
						Log.d("Server Insert", "key "+key+" value "+value);

						PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
						// sending ack
                        out.println(ACK);

					}

					else if(request.equals("read")){

						String key = in.readLine();

						String fileContent = openFile(key);

						PrintWriter outReturn = new PrintWriter(clientSocket.getOutputStream(), true);

						Log.d("Server read", "input key "+key);
						Log.d("Server read", "key "+ key + "value "+ fileContent);

						outReturn.println(fileContent);

					}

					else if(request.equals("queryAll")){

						HashMap<String, String> localFiles = readLocalFiles();

						String numFiles = String.valueOf(localFiles.size());
						PrintWriter outReturn = new PrintWriter(clientSocket.getOutputStream(), true);

						outReturn.println(numFiles);

						// send files
						for(Map.Entry<String, String> file: localFiles.entrySet()){

							// send key
							outReturn.println(file.getKey());

							// send value
							outReturn.println(file.getValue());

						}

					}

					else if(request.equals("delete")){

						String key = in.readLine();
						Context context = getContext();
						context.deleteFile(key);

                        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

                        // sending ACK
                        out.println(ACK);

					}

					else if(request.equals("deleteAll")){

						deleteLocalFiles();
					}

					else if(request.equals("update")){

					    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

					    String updatePort = in.readLine();
					    List<String> missedPortInsert = missedInsert.get(updatePort);

					    // sending number of inserts
                        out.println(missedPortInsert.size());

					    for(String insert: missedPortInsert){

					        String[] values = insert.split(",");
					        String key = values[0];
					        String value = values[1];

					        // send key
                            out.println(key);

                            // send value
                            out.println(value);
                        }

					    // clearing the updated inserts for this update port
					    missedInsert.get(updatePort).clear();

					    List<String> missedPortDelete = missedDelete.get(updatePort);

					    // sending number of delete
                        out.println(missedPortDelete.size());

                        for(String deleteKey: missedPortDelete){
                            // send delete key
                            out.println(deleteKey);

                        }

                        // clearing the updated delete for this update port
                        missedDelete.get(updatePort).clear();


                    }

				} catch (IOException e) {
					e.printStackTrace();
				} catch (NullPointerException e){
					e.printStackTrace();
				}
			}

		}
	}

}
