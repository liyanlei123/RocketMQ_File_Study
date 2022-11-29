package org.apache.rocketmq.demo;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class ConsumeQueueMessageReadTest {

    public static MappedByteBuffer mappedByteBuffer = null;
    private static int MESSAGE_COUNT = 10;

    public static void main(String[] args) throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(URI.create("file:/c:/123/consumerQueue.txt")),
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 409600);
        fileChannel.close();
        mappedByteBuffer.position(0);
        //根据索引下标读取索引，实际情况是用户消费的最新点位，存在在broker的偏移量文件中
        int index = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            mappedByteBuffer.position(i * 20);

            long commitlogOffset = mappedByteBuffer.getLong();
            // System.out.println(commitlogOffset);
            long msgLen = mappedByteBuffer.getInt();
            Long tag = mappedByteBuffer.getLong();
            //System.out.println("======读取到consumerQueue,commitlogOffset:"+commitlogOffset+",msgLen :"+msgLen+"===");
            //根据偏移量读取CommitLog
            System.out.println("=================commitlog读取第："+index+"消息，偏移量为" + commitlogOffset + "===================");

            readCommitLogByOffset(Integer.valueOf(commitlogOffset + ""));
            index ++;

        }
    }


    public static MappedByteBuffer initFileChannel() throws IOException {
        if (mappedByteBuffer == null) {
            FileChannel commitLogfileChannel = FileChannel.open(Paths.get(URI.create("file:/c:/123/commitLog.txt")),
                    StandardOpenOption.WRITE, StandardOpenOption.READ);

            mappedByteBuffer = commitLogfileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 409600);
            commitLogfileChannel.close();
        }

        return mappedByteBuffer;

    }


    /*
     *
     * 根据偏移量读取commitLog
     * */
    public static void readCommitLogByOffset(int offset) throws IOException {

        /* 存放顺序，读到时候保持顺序一致
           b.putLong(totalSize);//totalSize
            b.put(getBytes(msgId, 64));//msgId
            b.put(getBytes(topic, 64));//topic,定长64
            b.putLong(queueOffset);//索引偏移量
            b.putLong(msgBody.getBytes(StandardCharsets.UTF_8).length);//bodySize
            b.put(msgBody.getBytes(StandardCharsets.UTF_8));//body
            b.put(getBytes(transactionId, 64));
            b.putLong(commitLogOffset);//commitLogOffset
        */

        MappedByteBuffer mappedByteBuffer = initFileChannel();
        mappedByteBuffer.position(offset);


        long totalSize = mappedByteBuffer.getLong();//消息长度

        byte[] msgIdByte = new byte[64];//uuid 固定是64
        mappedByteBuffer.get(msgIdByte);

        byte[] topicByte = new byte[64];// 固定是64
        mappedByteBuffer.get(topicByte);
        long queueOffset = mappedByteBuffer.getLong();
        Long bodySize = mappedByteBuffer.getLong();
        int bSize = Integer.valueOf(bodySize + "");
        byte[] bodyByte = new byte[bSize];//bodySize 长度不固定
        mappedByteBuffer.get(bodyByte);
        byte[] transactionIdByte = new byte[64];//uuid 固定是64
        mappedByteBuffer.get(transactionIdByte);
        long commitLogOffset = mappedByteBuffer.getLong();//偏移量
        System.out.println("totalSize:" + totalSize);
        System.out.println("msgId:" + new String(msgIdByte));
        System.out.println("topic:" + new String(topicByte));
        System.out.println("queueOffset:" + queueOffset);
        System.out.println("bodySize:" + bodySize);
        System.out.println("body:" + new String(bodyByte));
        System.out.println("transactionId:" + new String(transactionIdByte));
        System.out.println("commitLogOffset:" + commitLogOffset);

    }

}
