package study;

import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Text_TEST {
    @Test
    public void text_test() {
        Text t = new Text("Hadoop");
        System.out.println(t.getLength());
        System.out.println(t.getBytes().length);
        System.out.println(t.charAt(2));
        System.out.println((char)t.charAt(2));
        System.out.println(t.charAt(100));
        System.out.println(t.find("do"));
        System.out.println(t.find("o"));
        System.out.println(t.find("o",4));
        System.out.println(t.find("pig"));
    }
    @Test
    public void string() throws UnsupportedEncodingException {
        String s = "\u0041\u00DF\u6771\uD801\uDC00";
        System.out.println(s.length()); //5
        System.out.println(s.getBytes(StandardCharsets.UTF_8).length);
        System.out.println(s.indexOf("\u0041"));
        System.out.println(s.indexOf("\u00DF"));
        System.out.println(s.indexOf("\uD801"));
        System.out.println(s.indexOf("\uDC00"));
        System.out.println(s.charAt(0));
        System.out.println(s.charAt(1));
        System.out.println(s.charAt(2));
        System.out.println(s.charAt(3));
        System.out.println(s.charAt(4));
    }
    //Text在非ASCII码字符与String的区别，以一个字节为find与at的下标，而不是一个字符
    @Test
    public void text() {
        Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
        System.out.println(t.getLength()); // 10
        System.out.println(t.find("\u0041"));
        System.out.println(t.find("\u00DF"));
        System.out.println(t.find("\u6771"));
        System.out.println(t.find("\uDC00"));
        System.out.println(Integer.toHexString(t.charAt(0)));
        System.out.println(Integer.toHexString(t.charAt(1)));
        System.out.println(Integer.toHexString(t.charAt(3)));
        System.out.println(Integer.toHexString(t.charAt(6)));
    }

    //Text的遍历--使用静态方法Text.bytesToCodePoint(ByteBuffer),需要先将text转为buf
    @Test
    public void TextIterator() {
        Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
        ByteBuffer buf = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
        int cp;
        while (buf.hasRemaining() && (cp = Text.bytesToCodePoint(buf)) != -1) {
            System.out.println(Integer.toHexString(cp));
        }
    }

    // 通过set方法可以修改Text中除NullWritable外的数据
    @Test
    public void testTextSet() {
        Text text = new Text("Hadoop");
        text.set("world");
        Assert.assertEquals(text.getLength(),5);
        Assert.assertEquals(text.getBytes().length,5);
    }

    @Test
    // Text序列化的格式为前一个字节指明后面的字节数，后面的内容即为字符本省
    public void TextSerialize() throws IOException{
        Text text = new Text("Hadoop test");
        byte[] bytes = serialize(text);
        System.out.println(StringUtils.byteToHexString(bytes));
        System.out.println("--------------");
        Text new_text = new Text();
        deserialize(new_text, bytes);
        System.out.println(new_text);
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

    @Test
    // ByteWritable的序列化为前4个字节指明字节数组长度
    public void ByteTest() throws IOException{
        BytesWritable bw = new BytesWritable(new byte[] {1,2,3,4});
        byte[] bytes = serialize(bw);
        System.out.println(StringUtils.byteToHexString(bytes));
        // 00000004（长度）01020304（内容）
    }
    @Test
    public void TestObjectWritable() throws IOException{
        Text text=new Text("\u0051");
        ObjectWritable objectWritable=new ObjectWritable(text);
        System.out.println(
                StringUtils.byteToHexString(serialize(objectWritable)));
        // 序列化时导入了封装类型的名字：00196f72672e6170616368652e6861646f6f702e696f2e5465787400196f72672e6170616368652e6861646f6f702e696f2e5465787401
        // 显然这会浪费很多空间
    }
    static class MyWritable extends GenericWritable{
        MyWritable(Writable writable) {
            set(writable);
        }
        public static Class<? extends Writable>[] CLASSES=null;
        static {
            CLASSES = (Class<? extends Writable>[]) new Class[] {Text.class};
        }
        @Override
        protected Class<? extends Writable>[] getTypes() {
            return CLASSES;
        }
    }
    @Test
    public void TestGenericWritable() throws IOException{
        Text text = new Text("\u0041\u0071");
        MyWritable myWritable = new MyWritable(text);
        System.out.println(StringUtils.byteToHexString(serialize(text)));
        System.out.println(StringUtils.byteToHexString(serialize(myWritable)));
        // 02 4171--text ,字节数--内容
        // 00 02 4171--generic ,类型数组索引号--字节数--内容
        // 显然这么比较下来，像比如ObjectWritable就节省了很多空间
    }
    @Test
    //Writable Collection的序列化
    //包括Writable的一维二维数组的序列化
    //Writable作为key-value对的整体Map序列化
    public void TestWritableCollection() throws IOException{
        MapWritable src = new MapWritable();
        src.put(new IntWritable(1), new Text("cat"));
        src.put(new VIntWritable(2), new LongWritable(163));
        MapWritable dest = new MapWritable();
        WritableUtils.cloneInto(dest, src);
        System.out.println((Text)dest.get(new IntWritable(1)));
        System.out.println((LongWritable)dest.get(new VIntWritable(2)));
        System.out.println(StringUtils.byteToHexString(serialize(dest)));
        MapWritable new1 = new MapWritable();
        deserialize(new1, serialize(src));
        System.out.println(new1.get(new IntWritable(1)));
    }

    // 自定义Writable类型
    public static class TextPair implements WritableComparable<TextPair> {
        private Text first;
        private Text second;
        // 三个重载的构造函数，空默认构造，传入String的构造，传入Text的构造
        public TextPair() {
            set(new Text(), new Text());
        }
        public TextPair(String first, String second) {
            set(new Text(first), new Text(second));
        }
        public TextPair(Text first, Text second) {
            set(first, second);
        }
        // 私有实例空构造的setter函数
        public void set(Text first, Text second) {
            this.first = first;
            this.second = second;
        }
        // 私有实例成员的getter函数
        public Text getFirst() {
            return first;
        }
        public Text getSecond() {
            return second;
        }
        // 实现抽象类Writable的write方法
        @Override
        public void write(DataOutput out) throws IOException{
            first.write(out);
            second.write(out);
        }
        // 实现抽象类Writable的readFields方法
        @Override
        public void readFields(DataInput in) throws IOException{
            first.readFields(in);
            second.readFields(in);
        }
        @Override
        public String toString() {
            return first + "\t" + second;
        }
        // 实现Comparable接口的compareTo方法
        // 比较二个TextPair对象的大小。这是对象级别的比较
        @Override
        public int compareTo(TextPair tp) {
            int cmp = first.compareTo(tp.first);
            if (cmp != 0) {
                return cmp;
            }
            return second.compareTo(tp.second);
        }

        // 为自定义类型实现比较器类Comparator
        public static class Comparator extends WritableComparator {
            // 创建Text的比较器对象，其作为Text的嵌套类实现
            private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
            // 在构造函数里调用父类的构造函数指定要比较的对象类型
            public Comparator() {
                super(TextPair.class);
            }
            @Override
            // 重写父类的compare方法
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                try {
                    // firstL1即为first对象序列化后的总字节数
                    int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1,s1);
                    int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2,s2);
                    int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                    if (cmp != 0) {
                        return cmp;
                    }
                    return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        }
        // 注册自定义比较器
        static {
            WritableComparator.define(TextPair.class, new Comparator());
        }
    }
    @Test
    public void testTextPair() throws IOException{
        TextPair tp1 = new TextPair("hello", "world");
        Text t1 = new Text("hello");
        Text t2 = new Text("worlD");
        TextPair tp2 = new TextPair(t1, t2);
        System.out.println(tp1.compareTo(tp2));
        byte[] b1 = serialize(tp1);
        byte[] b2 = serialize(tp2);
        System.out.println();
        System.out.println(new TextPair.Comparator().compare(b1, 0, b1.length, b2, 0, b2.length));
    }
}
