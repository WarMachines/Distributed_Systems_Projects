package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {
    static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.simpledht.provider";
    static final Uri URI = Uri.parse("content://" + PROVIDER_NAME + "/" + DBHelper.TABLE_Name);
    private DBHelper dbHelper;
    public static String localport;
    static final int SERVER_PORT = 10000;
    static String myNext;
    static String myPrev;
    //https://developer.android.com/reference/android/database/MatrixCursor
    MatrixCursor CursorToHold;
    // for the star operation to perform on the avds before returning back after following the ring
    boolean waitForAll = false;

    HashMap<String, Integer> caseMaps;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        final SQLiteDatabase db = dbHelper.getReadableDatabase();
        if(selection.equals("@"))
        {
            db.delete(DBHelper.TABLE_Name,null,null);
            return 0;
        }
        HandleDeleteQuery(db,selection);
        return 0;
    }


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        InsertKeysToNode(values.getAsString("key"), values.getAsString("value"));
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        SetMyCaseMaps();
        Context context = getContext();
        dbHelper = new DBHelper(context);

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        localport = String.valueOf(Integer.parseInt(portStr));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        if (localport.equals("5554"))
            return false;

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, localport + "##NODE_ARRIVED##5554");
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        final SQLiteDatabase db = dbHelper.getReadableDatabase();
        if (selection.equals("@")) {
            Cursor cur = db.query(DBHelper.TABLE_Name, projection, null, null, null, null, sortOrder);
            return cur;
        }
        return ResolveQuery(db, selection, "Query");
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    private void decode_message(String receivedMessage) {
        // extract action type to perform that action
        String[] messageComponents = receivedMessage.split("##");
        String actionName = messageComponents[0];
        int actionType = caseMaps.get(actionName);

        switch (actionType) {
            case 1:
                String nodeToConnect = messageComponents[1];
                Connect_Node_To_Ring(nodeToConnect);
                break;
            case 2:
                String receivedPrevNode = messageComponents[1].split("&&")[0];
                String receivedNextNode = messageComponents[1].split("&&")[1];
                Link_Nodes(receivedPrevNode, receivedNextNode);
                break;
            case 3:
                String key = messageComponents[1].split("&&")[0];
                String value = messageComponents[1].split("&&")[1];
                InsertKeysToNode(key, value);
                break;
            case 4:
                Log.i("test", Arrays.toString(messageComponents));
                String portRequested = messageComponents[1].split("&&")[0];
                String AllMessages = messageComponents[1].split("&&")[1];
                Select_ALL(AllMessages, portRequested);
                break;
            case 5:
                String portRequestedSelect = messageComponents[1].split("&&")[0];
                String selection = messageComponents[1].split("&&")[1];
                Select_One(selection, portRequestedSelect);
                break;
            case 6:
                String discoveredKey = messageComponents[1].split("&&")[0];
                String discoveredValue = messageComponents[1].split("&&")[1];
                processSingleResult(discoveredKey, discoveredValue);
                break;
            case 7:
                String portRequestedDelete = messageComponents[1].split("&&")[0];
                String selectionDelete = messageComponents[1].split("&&")[1];
                Delete_ALL(selectionDelete, portRequestedDelete);
                break;
            case 8:
                String portRequestedDeleteOne = messageComponents[1].split("&&")[0];
                String selectDeleteOne = messageComponents[1].split("&&")[1];
                Delete_One(selectDeleteOne,portRequestedDeleteOne);
            default:
        }

    }

    private void Connect_Node_To_Ring(String nodeToConnect) {
        try {
            String msgToSend;
            if (myNext == null && myPrev == null) {
                myPrev = nodeToConnect;
                myNext = nodeToConnect;
                msgToSend = localport + "&&" + localport;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##LINK_NODES##" + nodeToConnect);
                return;
            }
            String localPortHash = genHash(localport);

            String myPrevNodeHash = null;
            String nodeToConnectHash = genHash(nodeToConnect);
            if (myPrev != null) {
                myPrevNodeHash = genHash(myPrev);
            }

            if (myPrevNodeHash.compareTo(localPortHash) < 0) {
                if (myPrevNodeHash.compareTo(nodeToConnectHash) < 0 && localPortHash.compareTo(nodeToConnectHash) >= 0) {
                    ReOrderNodes(nodeToConnect);
                } else {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeToConnect + "##NODE_ARRIVED##" + myNext);
                }
            } else {
                if (localPortHash.compareTo(nodeToConnectHash) < 0 && myPrevNodeHash.compareTo(nodeToConnectHash) >= 0) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeToConnect + "##NODE_ARRIVED##" + myNext);
                } else {
                    String temp = myPrev;
                    myPrev = nodeToConnect;
                    msgToSend = "NULL" + "&&" + nodeToConnect;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##LINK_NODES##" + temp);
                    msgToSend = temp + "&&" + localport;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##LINK_NODES##" + nodeToConnect);
                }
            }
        } catch (NoSuchAlgorithmException ex) {
        }
    }

    private void ReOrderNodes(String nodeToConnect) {
        String msgToSend;
        String oldPredecessor = myPrev;
        myPrev = nodeToConnect;

        msgToSend = "NULL" + "&&" + nodeToConnect;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##LINK_NODES##" + oldPredecessor);
        msgToSend = oldPredecessor + "&&" + localport;

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##LINK_NODES##" + nodeToConnect);
    }

    private void Link_Nodes(String prevNodeToSet, String nextNodeToSet) {
        if (!prevNodeToSet.equals("NULL"))
            myPrev = prevNodeToSet;
        if (!nextNodeToSet.equals("NULL"))
            myNext = nextNodeToSet;
    }

    private void InsertKeysToNode(String Key, String Value) {
        try {
            final SQLiteDatabase db = dbHelper.getWritableDatabase();
            Cursor cur = db.query(DBHelper.TABLE_Name, null, "key=?", new String[]{Key}, null, null, null);
            if (cur.getCount() > 0) {
                db.delete(DBHelper.TABLE_Name, "key=?", new String[]{Key});
            }
            String msgToSend;
            ContentValues cv = SetContentObject(Key, Value);
            if (myNext == null && myPrev == null) {
                db.insert(DBHelper.TABLE_Name, null, cv);
                return;
            }

            String localPortHash = genHash(localport);
            String myPrevNodeHash = null;
            String keyHash = genHash(Key);
            if (myPrev != null) {
                myPrevNodeHash = genHash(myPrev);
            }

            msgToSend = Key + "&&" + Value;

            if (myPrevNodeHash.compareTo(localPortHash) <= 0) {
                if (myPrevNodeHash.compareTo(keyHash) < 0 && localPortHash.compareTo(keyHash) >= 0) {
                    db.insert(DBHelper.TABLE_Name, null, cv);
                } else {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##INSERT_KEY##" + myNext);
                }
            } else {
                if (localPortHash.compareTo(keyHash) < 0 && myPrevNodeHash.compareTo(keyHash) >= 0) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##INSERT_KEY##" + myNext);
                } else {
                    db.insert(DBHelper.TABLE_Name, null, cv);
                }
            }
        } catch (NoSuchAlgorithmException ex) {
        }

    }

    private ContentValues SetContentObject(String key, String value) {
        ContentValues cv = new ContentValues();
        cv.put("key", key);
        cv.put("value", value);
        return cv;
    }

    private Cursor ResolveQuery(SQLiteDatabase db, String selection, String Caller) {
        try {
                CursorToHold = new MatrixCursor(new String[]{"key", "value"});
                if (selection.equals("*")) {
                    Cursor cur = db.query(DBHelper.TABLE_Name, null, null, null, null, null, null);
                    if (cur.moveToFirst()) {
                        int keyIndex = cur.getColumnIndex("key");
                        int valueIndex = cur.getColumnIndex("value");
                        do {
                            String key = cur.getString(keyIndex);
                            String value = cur.getString(valueIndex);
                            CursorToHold.addRow(new Object[]{key, value});
                        } while (cur.moveToNext());
                    }

                    if (myNext != null) {
                        String msgToSend = localport + "&&" + "*";
                        db.close();
                        forwardQueryCalls("SELECT_ALL", msgToSend);
                    }

                    return CursorToHold;
                } else {
                    Cursor cur = db.query(DBHelper.TABLE_Name, null, "key=?", new String[]{selection}, null, null, null);
                    if (cur.getCount() > 0) {
                        return cur;
                    } else {
                        cur.close();
                        CursorToHold = new MatrixCursor(new String[]{"key", "value"});
                        String msgToSend = localport + "&&" + selection;
                        db.close();
                        forwardQueryCalls("SELECT", msgToSend);
                        return CursorToHold;
                    }
                }

        } catch (Exception ex) {
            CursorToHold.close();
            db.close();
        }

        return null;
    }

    private void Select_ALL(String msgToSend, String remotePort) {
        if (remotePort.equals(localport)) {
            Log.i("test","initial Port");
            msgToSend = msgToSend.substring(0, msgToSend.length() - 1);
            if(!msgToSend.contains("?")) {
                waitForAll = true;
                return;
            }

            String[] Messages = msgToSend.split("\\?");
            String key, value;
            for (String cv : Messages) {
                String[] keyValuePair = cv.split("=");
                key = keyValuePair[0].replace("*","");
                value = keyValuePair[1];
                CursorToHold.addRow(new Object[]{key, value});
            }
            Log.i("test", "reached with record"+CursorToHold.getCount());
            waitForAll = true;
            return;

        } else {
            msgToSend = remotePort + "&&" + msgToSend;
            Log.i("test", "reached" + remotePort);
            final SQLiteDatabase db = dbHelper.getReadableDatabase();
            Cursor cur = db.query(DBHelper.TABLE_Name, null, null, null, null, null, null);
            if (cur.moveToFirst()) {
                int keyIndex = cur.getColumnIndex("key");
                int valueIndex = cur.getColumnIndex("value");
                do {
                    String key = cur.getString(keyIndex);
                    String value = cur.getString(valueIndex);
                    msgToSend += key + "=" + value + "?";
                } while (cur.moveToNext());
            }
            cur.close();
            Log.i("test", "moved Forward" + myNext);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##SELECT_ALL##" + myNext);
        }
    }

    private void Select_One(String selection, String remotePort) {
        String msgToSend;
        try {
            final SQLiteDatabase db = dbHelper.getReadableDatabase();
            Cursor cur = db.query(DBHelper.TABLE_Name, null, "key=?", new String[]{selection}, null, null, null);
            if (cur.getCount() > 0) {
                if (cur.moveToFirst()) {
                    int keyIndex = cur.getColumnIndex("key");
                    int valueIndex = cur.getColumnIndex("value");
                    String key = cur.getString(keyIndex);
                    String value = cur.getString(valueIndex);
                    msgToSend = key + "&&" + value;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##KEY_DISCOVERED##" + remotePort);
                }
            } else {
                msgToSend = remotePort + "&&" + selection;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##SELECT##" + myNext);
            }
        } catch (Exception e) {
        }

    }

    private void forwardQueryCalls(String queryType, String msgToSend) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##" + queryType + "##" + myNext);
        while (!waitForAll) {}
        waitForAll = false;
    }

    private void processSingleResult(String key, String value) {
        CursorToHold.newRow().add("key", key)
                .add("value", value);
        waitForAll = true;
    }

    private void HandleDeleteQuery(SQLiteDatabase db, String selection)
    {
        try
        {
            if(selection.equals("*"))
            {
                db.delete(DBHelper.TABLE_Name,null,null);
                String msgToSend = localport + "&&" + selection;
                forwardQueryCalls("DELETE_ALL",msgToSend);
            }
            else
            {
                Cursor cur = db.query(DBHelper.TABLE_Name, null, "key=?", new String[]{selection}, null, null, null);
                if (cur.getCount() > 0) {
                    db.delete(DBHelper.TABLE_Name,"key=?",new String[]{selection});
                }
                else
                {
                    String msgToSend = localport + "&&" + selection;
                    forwardQueryCalls("DELETE",msgToSend);
                }
            }
        }
        catch (Exception ex)
        {}

    }
    //https://developer.android.com/reference/android/content/ContentProvider.html#delete(android.net.Uri,%20java.lang.String,%20java.lang.String[])
    private void Delete_ALL(String msgToSend, String remotePort) {
        if (remotePort.equals(localport)) {
            waitForAll = true;
        } else {
            final SQLiteDatabase db = dbHelper.getReadableDatabase();
            db.delete(DBHelper.TABLE_Name,null,null);
            msgToSend = remotePort+"&&"+msgToSend;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##DELETE_ALL##" + myNext);
        }
    }
    //https://developer.android.com/reference/android/content/ContentProvider.html#delete(android.net.Uri,%20java.lang.String,%20java.lang.String[])
    private void Delete_One(String selection, String remotePort)
    {
        try {
            if (remotePort.equals(localport)) {
                waitForAll = true;
            } else {
                final SQLiteDatabase db = dbHelper.getReadableDatabase();
                Cursor cur = db.query(DBHelper.TABLE_Name, null, "key=?", new String[]{selection}, null, null, null);
                if (cur.getCount() > 0) {
                    db.delete(DBHelper.TABLE_Name,"key=?",new String[]{selection});
                    String msgToSend = remotePort + "&&" + selection;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##DELETE##" + remotePort);
                }
                else
                {
                    String msgToSend = remotePort + "&&" + selection;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend + "##DELETE##" + myNext);
                }
            }
        }
        catch (Exception ex)
        {}
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            while (true) {
                try {
                    Socket server = serverSocket.accept();
                    InputStreamReader inputStreamReader = new InputStreamReader(server.getInputStream());
                    BufferedReader reader = new BufferedReader(inputStreamReader);

                    String msg = reader.readLine();
                    while (msg == null || !msg.endsWith("EOF")) {
                        msg = reader.readLine();
                    }

                    // remove the eof indicator
                    msg = msg.substring(0, msg.length() - 3);
                    PrintWriter printWriter = new PrintWriter(server.getOutputStream());
                    printWriter.append("message received\n");
                    printWriter.flush();
                    inputStreamReader.close();
                    reader.close();
                    printWriter.close();
                    decode_message(msg);
                } catch (Exception ex) {
                    Log.e(TAG, ex.getMessage());
                }
            }

        }

        protected void onProgressUpdate(String... strings) {
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            int retryCount = 15;
            try {
                String[] Parameters = msgs[0].split("##");
                String ActionType = Parameters[1];
                String remotePort = Parameters[2];
                String VariableContent = Parameters[0];
                Log.i("test",remotePort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort) * 2);
                String msgToSend = ActionType + "##" + VariableContent + "EOF\n";
                InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
                BufferedReader reader = new BufferedReader(inputStreamReader);
                String ack = null;
                while (ack == null && retryCount > 0) {
                    printWriter.append(msgToSend);
                    printWriter.flush();
                    ack = reader.readLine();
                    retryCount--;
                }
                printWriter.close();
                reader.close();
                inputStreamReader.close();
                socket.close();
                socket.close();
            } catch (SocketException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    public class DBHelper extends SQLiteOpenHelper {
        public static final String Key_Column = "key";
        public static final String Value_Column = "value";
        public static final String DB_Name = "SimpleDht.db";
        public static final int DB_Version = 1;
        public static final String TABLE_Name = "messeges";

        public DBHelper(Context context) {
            super(context, DB_Name, null, DB_Version);
        }

        private static final String CREATE_MESSAGES_TABLE = "CREATE TABLE " + TABLE_Name + " (" + Key_Column + " INTEGER, " +
                Value_Column + " TEXT)";


        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(CREATE_MESSAGES_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            onUpgrade(db, oldVersion, newVersion);
        }
    }

    private void SetMyCaseMaps() {
        caseMaps = new HashMap<String, Integer>();
        caseMaps.put("NODE_ARRIVED", 1);
        caseMaps.put("LINK_NODES", 2);
        caseMaps.put("INSERT_KEY", 3);
        caseMaps.put("SELECT_ALL", 4);
        caseMaps.put("SELECT", 5);
        caseMaps.put("KEY_DISCOVERED", 6);
        caseMaps.put("DELETE_ALL", 7);
        caseMaps.put("DELETE",8);
    }

}