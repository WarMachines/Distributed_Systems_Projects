package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.net.Uri;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;
import android.os.AsyncTask;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import android.util.Log;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String[] REMOTE_PORTS_ARRAY= {"11108", "11112", "11116", "11120", "11124"};
    static List<String> REMOTE_PORTS = new ArrayList<String>(Arrays.asList(REMOTE_PORTS_ARRAY));
    //static final String[] REMOTE_PORTS= {"11108"};
    private static Uri mUri;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private ContentResolver contentResolver;
    static int key = 0;
    String FAILED_NODE = "";
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        contentResolver = getContentResolver();
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
            return;
        }


        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                EditText editText = (EditText) findViewById(R.id.editText1);
                String msg = editText.getText().toString();
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        });
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
    }

    private ContentValues SetContentObject(String message,String time_stamp) {
        ContentValues cv = new ContentValues();
        cv.put(KEY_FIELD, Integer.toString(key));
        cv.put(VALUE_FIELD,message);
        cv.put("time_stamp",time_stamp);
        return cv;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            while(true)
            {
                try {
                    Socket server = serverSocket.accept();
                    InputStreamReader inputStreamReader = new InputStreamReader(server.getInputStream());
                    BufferedReader reader = new BufferedReader(inputStreamReader);

                    String msg = reader.readLine();
                    while (msg == null || !msg.endsWith("EOF")) {
                        msg = reader.readLine();
                    }
                    msg = msg.substring(0,msg.length()-3);
                    PrintWriter printWriter = new PrintWriter(server.getOutputStream());

                    printWriter.append("message received\n");
                    printWriter.flush();
                    inputStreamReader.close();
                    reader.close();
                    printWriter.close();
                    //server.close();
                    saveContentValues(msg);
                    //publishProgress(msg);
                    publishProgress( msg.split("::")[0]);
                }
                catch (Exception ex)
                {
                    Log.e(TAG, ex.getMessage());
                }
            }

        }

        protected void onProgressUpdate(String...strings) {
            String strReceived = strings[0].trim();
            TextView textView = (TextView) findViewById(R.id.textView1);
            textView.append(strReceived + "\t\n");
            return;
        }

        private void saveContentValues(String message)
        {
            String[] data = message.split("::");
            contentResolver.insert(mUri,SetContentObject(data[0],data[1]));
            key = key +1;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Timestamp time_stamp = new Timestamp(System.currentTimeMillis());
            List<String> remote_port_temp = new ArrayList<String>(REMOTE_PORTS);
            for (String remote_port: remote_port_temp) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));
                    String msgToSend = msgs[0] + "::" + time_stamp+"EOF\n";
                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader reader = new BufferedReader(inputStreamReader);
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
                    String ack = null;
                    int count = 0;
                    while(ack == null && count<50)
                    {
                        printWriter.append(msgToSend);
                        printWriter.flush();
                        ack = reader.readLine();
                        count++;
                    }

                    if (count >= 50)
                        REMOTE_PORTS.remove(remote_port);

                    printWriter.close();
                    reader.close();
                    inputStreamReader.close();
                    socket.close();
                    socket.close();
                } catch (SocketException e){
                    Log.e(TAG, "ClientTask socket IOException");
                    REMOTE_PORTS.remove(remote_port);
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                    REMOTE_PORTS.remove(remote_port);
                }
            }
            return null;
        }
    }
}
