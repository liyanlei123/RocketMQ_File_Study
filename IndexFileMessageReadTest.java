package org.apache.rocketmq.demo;

import java.io.IOException;
import java.net.URI;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class IndexFileMessageReadTest {

    public static MappedByteBuffer mappedByteBuffer = null;

    public static void main(String[] args) throws IOException {
        String msgId = "9a508d90-30f6-4a25-812f-25d750736afe";
        readByMessageId(msgId);

    }

    private static void readByMessageId(String messageId) throws IOException {
        FileChannel indexFileChannel = FileChannel.open(Paths.get(URI.create("file:/c:/123/index.txt")),
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        MappedByteBuffer indexMappedByteBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
        indexFileChannel.close();

        System.out.println("============get indexFile header===============");
        System.out.println("beginTimestampIndex:" + indexMappedByteBuffer.getLong());
        System.out.println("endTimestampIndex:" + indexMappedByteBuffer.getLong());
        System.out.println("beginPhyoffsetIndex:" + indexMappedByteBuffer.getLong());
        System.out.println("endPhyoffsetIndex:" + indexMappedByteBuffer.getLong());
        System.out.println("hashSlotcountIndex:" + indexMappedByteBuffer.getInt());
        System.out.println("indexCountIndex:" + indexMappedByteBuffer.getInt());
        System.out.println("");

        int keyHash = messageId.hashCode();

        //取模，计算第几个槽位
        int slotPos = keyHash % 100 > 0 ? keyHash % 100 : -1 * (keyHash % 100);
        System.out.println("messageId:" + messageId + ",取模为:" + slotPos);

        // slot的文件偏移量（字节长度）
        int absSlotPos = 40 + slotPos * 4;
        System.out.println("哈希槽的字节数组位置:(40+" + slotPos + "*4)=" + absSlotPos);


        //获取hash槽上存取的件索引,第几个文件
        int index = indexMappedByteBuffer.getInt(absSlotPos);

        //计算数据需要存储的文件偏移量（字节长度）
        int absIndexPos =
                40 + 100 * 4
                        + index * 20;

        System.out.println("第几个文件index=" + index + "，实际存储数据的字节数组位置:(40 + 100 * 4+index *20)=" + absIndexPos);

        long keyHash1 = indexMappedByteBuffer.getInt(absIndexPos);
        long pyhOffset = indexMappedByteBuffer.getLong(absIndexPos + 4);
        int timeDiff = indexMappedByteBuffer.getInt(absIndexPos + 4 + 8);
        int preIndexNo = indexMappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);


        System.out.println("从index获取到的commitLog偏移量为:" + pyhOffset);
        System.out.println("");

        readCommitLogByOffset((int) pyhOffset);

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
     * 根据偏移量读取CcommitLog
     * */
    public static void readCommitLogByOffset(int offset) throws IOException {


        /*b.putLong(totalSize);//totalSize
            b.put(getBytes(msgId, 64));//msgId
            b.put(getBytes(topic, 64));//topic,定长64
            b.putLong(queueOffset);//索引偏移量
            b.putLong(msgBody.getBytes(StandardCharsets.UTF_8).length);//bodySize
            b.put(msgBody.getBytes(StandardCharsets.UTF_8));//body
            b.put(getBytes(transactionId, 64));
            b.putLong(commitLogOffset);//commitLogOffset
        */
        System.out.println("=================commitlog读取偏移量为" + offset + "的消息===================");

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


    public static byte[] toByteArray(long number) {
        byte length = Long.BYTES;
        byte[] bytes = new byte[length];

        for (byte i = 0; i < length; i++) {
            bytes[length - 1 - i] = (byte) number;
            number >>= 8;
        }

        return bytes;

    }


}
