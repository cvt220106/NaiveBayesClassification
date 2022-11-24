package study;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import org.mortbay.util.StringUtil;

import java.io.*;

public class WritableDemo {
    public static void main(String[] args) throws IOException{
        IntWritable writable = new IntWritable(163);
        byte[] bytes = serialize(writable);
        System.out.print("[");
        for(byte b : bytes) {
            System.out.print(b + " ");
        }
        System.out.println("]");
        System.out.println(StringUtils.byteToHexString(bytes));
        assert bytes.length!=4:"error";
        assert StringUtils.byteToHexString(bytes)!="000000a3";
        IntWritable writable1 = new IntWritable();
        deserialize(writable1, bytes);
        System.out.println(writable1.get());
    }
    public static byte[] serialize(Writable writable) throws  IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }
    public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return bytes;
    }

}
