package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;
import android.os.AsyncTask;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.UnknownHostException;
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
    static final String[] REMOTE_PORTS= {"11108", "11112", "11116","11120","11124"};
    //static final String[] REMOTE_PORTS= {"11108"};
    private static Uri mUri;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private ContentResolver contentResolver;
    static int key = 0;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        contentResolver = getContentResolver();
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
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
            String msg = editText.getText().toString() + "\n";
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

    private ContentValues SetContentObject(String message) {
        ContentValues cv = new ContentValues();
        cv = new ContentValues();
        cv.put(KEY_FIELD, Integer.toString(key));
        cv.put(VALUE_FIELD,message);
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
                    Socket connSocket = serverSocket.accept();
                    DataInputStream ds = new DataInputStream(connSocket.getInputStream());
                    String msg = ds.readUTF();
                    saveContentValues(msg);
                    publishProgress(msg);
                    ds.close();
                    String ack = "message received";
                    DataOutputStream dos = new DataOutputStream(connSocket.getOutputStream());
                    dos.writeUTF(ack);
                }
                catch (Exception ex)
                {}
            }

        }

        protected void onProgressUpdate(String...strings) {
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            return;
        }

        private void saveContentValues(String value)
        {
            contentResolver.insert(mUri,SetContentObject(value));
            key = key +1;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            for (String remote_port: REMOTE_PORTS) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));

                    String msgToSend = msgs[0];
                    DataOutputStream ds = new DataOutputStream(socket.getOutputStream());
                    ds.writeUTF(msgToSend);
                    Log.d("Send", msgToSend);
                    String ack = "";
                    while (ack.isEmpty()) {
                        DataInputStream dis = new DataInputStream(socket.getInputStream());
                        ack = dis.readUTF();
                        if (!ack.isEmpty()) {
                            break;
                        }
                    }
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            return null;
        }
    }
}
