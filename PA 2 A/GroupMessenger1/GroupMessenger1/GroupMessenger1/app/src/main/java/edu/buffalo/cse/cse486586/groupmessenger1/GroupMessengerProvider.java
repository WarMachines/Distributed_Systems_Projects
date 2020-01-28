package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {
    static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.groupmessenger1.provider";
    static final Uri URI = Uri.parse("content://" + PROVIDER_NAME+ "/"+ DBHelper.TABLE_Name);
    private DBHelper dbHelper;


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         * 
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */;
         //https://developer.android.com/guide/topics/providers/content-provider-basics
        final SQLiteDatabase db = dbHelper.getWritableDatabase();
        if(values.getAsString("key").equals("key0") || values.getAsString("key").equals("0"))
        {
            db.delete(DBHelper.TABLE_Name,null,null);
        }
        long returnId=db.insert(DBHelper.TABLE_Name,null,values);
        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        Context context = getContext();
        dbHelper = new DBHelper(context);
        return false;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */

        //https://developer.android.com/guide/topics/providers/content-provider-basics
        final SQLiteDatabase db = dbHelper.getReadableDatabase();
        Cursor cur = db.query(DBHelper.TABLE_Name,projection,"key=?",new String[] {selection},null,null,sortOrder);
        Log.v("querySelection", selection);
        return cur;
    }

    public class DBHelper extends SQLiteOpenHelper{
        public static final String Key_Column = "key";
        public static final String Value_Column = "value";
        public static final String DB_Name = "mutlicast.db";
        public static final int DB_Version= 1;
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
}
