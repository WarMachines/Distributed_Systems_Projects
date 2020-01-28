package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.w3c.dom.Node;

import javax.xml.validation.Validator;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

    static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    static final Uri URI = Uri.parse("content://" + PROVIDER_NAME );
    public String[] nodes = {"5562", "5556", "5554", "5558", "5560"};
    HashMap<String,String> myKeys;
    public static String localport;
    static final int SERVER_PORT = 10000;
    HashMap<String, Integer> caseMaps;
    boolean flag = false;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        while(flag)
        {}
        if(selection.equals("@")) {
            myKeys.clear();
            return 0;
        }
        else
        {
            HandleDeleteQuery(selection);
        }
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
        while(flag)
        {}
        Log.i("insert",values.getAsString("key"));
        insertKeysToNode(values.getAsString("key"), values.getAsString("value"));
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        SetMyCaseMaps();
        myKeys = new HashMap<String, String>();
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
        new NodeJoinTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        while(flag)
        {}
        MatrixCursor CursorToHold;
        if(selection.equals("@")) {
            CursorToHold = new MatrixCursor(new String[]{"key", "value"});
            for(String key: myKeys.keySet())
            {
                CursorToHold.addRow(new Object[]{key, myKeys.get(key)});
            }
            return CursorToHold;
        }
        else
        {
            return HandleQuery(selection);
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private void insertKeysToNode(String Key, String Value)
    {
        try {
            int i = getNodeIndex(Key);
            int count = 3;
            if (localport.equals(nodes[i % 5])) {
                Log.i("message saved:", Key);
                insertIntoDB(Key, Value);
                count--;
                i++;
            }
            Log.i("Insert","started");
            String msgToSend = Key + "&&" + Value;
            while (count > 0) {
                Log.i("message forwarded:", Key);
                sendMsg(nodes[i % 5], "INSERT_KEY##" + msgToSend);
                count--;
                i++;
            }
            Log.i("Insert","ended");
        }
        catch (Exception ex)
        {

        }
    }

    private void insertIntoDB(String Key, String Value)
    {
        Log.i("message inserted to DB:", localport+Key);
        myKeys.put(Key, Value);
    }

    private void HandleDeleteQuery(String selection)
    {
        Log.i("select",selection);
        if(selection.equals("*"))
        {
            myKeys.clear();
            int i;
            for(i = 0; i <nodes.length; i++)
            {
                String msgToSend = "*";
                sendMsg(nodes[i%5],"DELETE##"+msgToSend);
            }
        }
        else
        {
            int i = getNodeIndex(selection);
            Log.i("select",Integer.toString(i));
            int count = 3;
            if (localport.equals(nodes[i % 5])) {
                myKeys.remove(selection);
                count--;
                i++;
            }

            String msgToSend = selection;
            while (count > 0) {
                Log.e("select from",nodes[i % 5]);
                sendMsg(nodes[i % 5], "DELETE##" + msgToSend);
                count--;
                i++;
            }
        }
    }

    private Cursor HandleQuery(String selection)
    {
        MatrixCursor CursorToHold;
        Log.i("select Handle Query",selection);
        CursorToHold = new MatrixCursor(new String[]{"key", "value"});
        if(selection.equals("*"))
        {
            StringBuilder messages = new StringBuilder();
            messages.append(SelectKeys("SELECT##*"));
            int i;
            for(i = 0; i <nodes.length; i++)
            {
                if(nodes[i] == localport)
                    continue;
                String msgToSend = "*";
                messages.append(sendMsg(nodes[i%5],"SELECT##"+msgToSend));
            }
            if(messages.length()>0)
                messages.delete(messages.length()-1,messages.length());

            String msgs = messages.toString();
            String[] Allmessages = msgs.split(" ");
            Log.i("msgs",msgs);
            for(String msg: Allmessages)
            {
                String key = msg.split("&&")[0];
                String value = msg.split("&&")[1];
                CursorToHold.addRow(new Object[]{key,value});
            }
        }
        else
        {
            Log.i("select query for key",selection);
            int i = getNodeIndex(selection);
            Log.i("select query for node",Integer.toString(i));
            int count = 3;
            HashMap<String,Integer> local = new HashMap<String, Integer>();
            String valueToReturn = null;
            if (localport.equals(nodes[i % 5])) {
                local.put(myKeys.get(selection),1);
                valueToReturn = myKeys.get(selection);
                count--;
                i++;
            }

            String msgToSend = selection;
            while (count > 0) {
                String msgRecieved = sendMsg(nodes[i%5],"SELECT##"+msgToSend);
                if(msgRecieved!=null && msgRecieved.length()>0)
                {
                    String localValue = msgRecieved.split("&&")[1];
                    Log.i("resulteachquery", selection+":"+localValue);
                    if(local.containsKey(localValue))
                    {
                        valueToReturn = localValue;
                        break;
                    }
                    else
                    {
                        if(valueToReturn == null)
                            valueToReturn = localValue;
                        local.put(localValue,1);
                    }
                }
                count--;
                i++;
            }
            Log.i("select result", selection+":"+valueToReturn);
            CursorToHold.addRow(new Object[]{selection, valueToReturn});
        }

        return  CursorToHold;
    }

    private String SelectKeys(String msg)
    {
//        Log.e("select",msg);
        String selection = msg.split("##")[1];
        StringBuilder sb = new StringBuilder();
        if(selection.equals("*"))
        {
            for(String key: myKeys.keySet())
            {
                sb.append(key+"&&"+myKeys.get(key));
                sb.append(" ");
            }
        }
        else
        {
            sb.append(selection+"&&"+myKeys.get(selection));
        }
        return sb.toString();
    }

    private String SelectedNodesMessages(String msg)
    {
        String node = msg.split("##")[1];
        StringBuilder sb = new StringBuilder();
        try {
            String nodeHash = genHash(node);
            String nodePrevHash =  genHash(nodes[((getNodeIndex(node)+5)-1) % 5]);
            if(!node.equals(nodes[0])) {
                for (String key : myKeys.keySet()) {
                    String keyHash = genHash(key);
                    if (keyHash.compareTo(nodeHash) <= 0 && keyHash.compareTo(nodePrevHash) > 0) {
                        sb.append(key + "&&" + myKeys.get(key));
                        sb.append(" ");
                    }
                }
            }
            else
            {
                for (String key : myKeys.keySet()) {
                    String keyHash = genHash(key);
                    if (keyHash.compareTo(nodeHash) <= 0 ||(keyHash.compareTo(nodePrevHash) > 0)) {
                        sb.append(key + "&&" + myKeys.get(key));
                        sb.append(" ");
                    }
                }
            }
        }
        catch (Exception ex)
        {
        }
        return sb.toString();
    }

    private void RecoverMessages()
    {
        // get its messages from replicated nodes
        String messages = new String();
        int nodeIndex = getNodeIndex(localport);
        int i;
        String msgToSend;
        for(i=nodeIndex+1;i<=nodeIndex+2;i++)
        {
            msgToSend = localport;
            String temp = (sendMsg(nodes[i % 5], "RECOVER##" + msgToSend));
            if(temp !=null && !temp.equals("null"))
               messages+=temp;
        }

        Log.i("current node",Integer.toString(nodeIndex));
        for(i=nodeIndex+4;i>=nodeIndex+3;i--)
        {
            Log.i("replicate from",nodes[i % 5]);
            msgToSend = nodes[i % 5];
            String temp = (sendMsg(nodes[i % 5], "RECOVER##" + msgToSend));
            if(temp !=null && !temp.equals("null"))
                messages+=temp;
        }
        Log.i("messages recovered", messages);
        if(!messages.equals("null") && messages.length()>0)
        {
            Log.i("sasa",Integer.toString(messages.length()));
            String msgs = messages.substring(0,messages.length()-1);
            String[] Allmessages = msgs.split(" ");
            Log.i("recover",msgs);
            for(String msg: Allmessages)
            {
                String key = msg.split("&&")[0];
                String value = msg.split("&&")[1];
                if(myKeys.containsKey(key))
                    continue;
                myKeys.put(key,value);
            }

        }
    }

    private void DeleteKeys(String selection)
    {
        if(selection.equals("*"))
        {
            myKeys.clear();
        }
        else
        {
            myKeys.remove(selection);
        }
    }

    private int getNodeIndex(String Key)
    {
        int i = 0;
        try {
            String keyHash = genHash(Key);
            for (i = 0; i < nodes.length; i++) {
                if (keyHash.compareTo(genHash(nodes[i])) > 0)
                    continue;
                break;
            }
        }
        catch (Exception ex)
        {
        }
        return i;
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
                break;
            case 2:
                String receivedPrevNode = messageComponents[1].split("&&")[0];
                String receivedNextNode = messageComponents[1].split("&&")[1];
                break;
            case 3:
                String key = messageComponents[1].split("&&")[0];
                String value = messageComponents[1].split("&&")[1];
                insertIntoDB(key,value);
                break;
            case 4:
                String portRequested = messageComponents[1].split("&&")[0];
                String AllMessages = messageComponents[1].split("&&")[1];
                break;
            case 5:
                String portRequestedSelect = messageComponents[1].split("&&")[0];
                String selection = messageComponents[1].split("&&")[1];
                break;
            case 6:
                String selectionDelete = messageComponents[1];
                DeleteKeys(selectionDelete);
                break;
            default:
        }

    }

    private void SetMyCaseMaps() {
        caseMaps = new HashMap<String, Integer>();
        caseMaps.put("NODE_ARRIVED", 1);
        caseMaps.put("LINK_NODES", 2);
        caseMaps.put("INSERT_KEY", 3);
        caseMaps.put("SELECT", 4);
        caseMaps.put("KEY_DISCOVERED", 5);
        caseMaps.put("DELETE", 6);
    }

    private String sendMsg(String remotePort,String msgToSend) {
        String msg = null;
        Log.i("send messages", remotePort+msgToSend);
        try {
            int retryCount = 30;
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(remotePort) * 2);
            msgToSend = msgToSend + "EOF\n";
            InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
            PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
            printWriter.append(msgToSend);
            printWriter.flush();
            BufferedReader reader = new BufferedReader(inputStreamReader);


            if(!msgToSend.toLowerCase().contains("select") && !msgToSend.toLowerCase().contains("recover")) {
                while (msg == null && retryCount > 0) {
                    msg = reader.readLine();
                    retryCount--;
                }
            }
            else
            {
                while (msg == null || !msg.endsWith("EOF")) {
                    if(retryCount < 0)
                        break;
                    msg = reader.readLine();
                    retryCount--;
                }
                if(msg!=null && msg.length()>0)
                    msg = msg.substring(0, msg.length() - 3);
            }
            printWriter.close();
            reader.close();
            inputStreamReader.close();
            socket.close();
            socket.close();
        } catch (
                SocketException e) {
            Log.i(TAG, "ClientTask socket IOException");
        } catch (
                UnknownHostException e) {
            Log.i(TAG, "ClientTask UnknownHostException");
        } catch (
                IOException e) {
            Log.i(TAG, "ClientTask socket IOException");
        }
        return msg == null ? "" : msg;
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
                    PrintWriter printWriter = new PrintWriter(server.getOutputStream());
                    // remove the eof indicator
                    msg = msg.substring(0, msg.length() - 3);
                    if(msg.toLowerCase().contains("select")) {
                        String localMessage = SelectKeys(msg)+"EOF\n";
                        printWriter.append(localMessage);
                    }
                    else if(msg.toLowerCase().contains("recover"))
                    {
                        Log.i("recovery message for node", msg);
                        String localMessage = SelectedNodesMessages(msg)+"EOF\n";
                        printWriter.append(localMessage);
                    }
                    else
                    {
                        printWriter.append("message received\n");
                    }
                    printWriter.flush();
                    inputStreamReader.close();
                    reader.close();
                    printWriter.close();
                    if(!msg.toLowerCase().contains("select") && !msg.toLowerCase().contains("recover"))
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

    private class NodeJoinTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            flag = true;
            Log.i("recover task", "called");
            RecoverMessages();
            flag = false;
            return null;
        }
    }
}
