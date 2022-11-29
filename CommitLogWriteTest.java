package org.apache.rocketmq.demo;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.test.smoke.model.ConsumerQueueData;
import org.apache.rocketmq.test.smoke.model.IndexFileItemData;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class CommitLogWriteTest {

    private static Long commitLogOffset = 0L;//8byte(commitlog offset)
    private static List<ConsumerQueueData> consumerQueueDatas = new ArrayList<>();
    private static List<IndexFileItemData> indexFileItemDatas = new ArrayList<>();
    private static int MESSAGE_COUNT = 10;

    public static void main(String[] args) throws IOException {
        createCommitLog();
        createConsumerQueue();
        createIndexFile();
    }

    private static void createCommitLog() throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(URI.create("file:/c:/123/commitLog.txt")),
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 409600);
        fileChannel.close();
        Random random = new Random();

        int count = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String topic = "Topic-test";
            String msgId = UUID.randomUUID().toString();
            String msgBody = "消息内容" + "msgmsgmsgmsgmsgmsgmsgmsgmsgmsgmsgmsgmsgmsgmsgmsgmsg".substring(0, random.nextInt(48) + 1);//
            long queueOffset = i;//索引偏移量
            String transactionId = UUID.randomUUID().toString();


         /* 数据格式，位置固定
         int totalSize;//消息长度
         String msgId;
         String topic;
         long queueOffset;//索引偏移量
         long bodySize;//消息长度
         byte[] body;//消息内容
         String transactionId;
         long commitLogOffset;//从第一个文件开始算的偏移量

         */

            int msgTotalLen = 8 //msgTotalLen field
                    + 64  //msgId field长度
                    + 64 //topic field长度
                    + 8 //索引偏移量field长度
                    + 8 //消息长度field长度
                    + msgBody.getBytes(StandardCharsets.UTF_8).length //field
                    + 64  //transactionId field长度
                    + 64  //commitLogOffset field长度;
                    ;

            //ByteBuffer b = ByteBuffer.allocate(msgTotalLen);
            // //如果3个消息长度分别是100，200，350，则偏移量分别是0,100,300
            mappedByteBuffer.position(Integer.valueOf(commitLogOffset + ""));

            mappedByteBuffer.putLong(msgTotalLen);//msgTotalLen
            mappedByteBuffer.put(getBytes(msgId, 64));//msgId
            mappedByteBuffer.put(getBytes(topic, 64));//topic,定长64
            mappedByteBuffer.putLong(queueOffset);//索引偏移量
            mappedByteBuffer.putLong(msgBody.getBytes(StandardCharsets.UTF_8).length);//bodySize
            mappedByteBuffer.put(msgBody.getBytes(StandardCharsets.UTF_8));//body
            mappedByteBuffer.put(getBytes(transactionId, 64));
            mappedByteBuffer.putLong(commitLogOffset);//commitLogOffset
            //b.flip();
            //mappedByteBuffer.put(b);


            System.out.println("写入消息，第:" + i + "次");

            System.out.println("msgTotalLen:" + msgTotalLen);
            System.out.println("msgId:" + msgId);
            System.out.println("topic:" + topic);
            System.out.println("msgBody:" + msgBody);
            System.out.println("transactionId:" + transactionId);
            System.out.println("commitLogOffset:" + commitLogOffset);

            ConsumerQueueData consumerQueueData = new ConsumerQueueData();
            consumerQueueData.setOffset(commitLogOffset);
            consumerQueueData.setMsgLength(msgTotalLen);
            consumerQueueData.setTagCode(100L);
            //准备生成consumeQueue文件
            consumerQueueDatas.add(consumerQueueData);

            commitLogOffset = msgTotalLen + commitLogOffset;


            IndexFileItemData indexFileItemData = new IndexFileItemData();
            indexFileItemData.setKeyHash(msgId.hashCode());
            indexFileItemData.setMessageId(msgId);
            indexFileItemData.setPhyOffset(commitLogOffset);
            //准备生成indexFile文件
            indexFileItemDatas.add(indexFileItemData);
            mappedByteBuffer.force();
            count++;
        }

        System.out.println("commitLog数据保存完成,totalSize:" + count);

    }


    public static void createConsumerQueue() throws IOException {
        System.out.println("");
        System.out.println("ConsumerQueue file create!" );

        FileChannel fileChannel = FileChannel.open(Paths.get(URI.create("file:/c:/123/consumerQueue.txt")),
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
        fileChannel.close();
        int count = 0;
        for (int i = 0; i < consumerQueueDatas.size(); i++) {
            ConsumerQueueData consumerQueueData = consumerQueueDatas.get(i);
            //指定写入位置
            mappedByteBuffer.position(i * 20);
            mappedByteBuffer.putLong(consumerQueueData.getOffset());//8byte(commitlog offset)
            mappedByteBuffer.putInt(consumerQueueData.getMsgLength());//4byte （msgLength)
            mappedByteBuffer.putLong(consumerQueueData.getTagCode());//8byte (tagCode)

            count++;
            System.out.println("consumerQueue数据写入完成:" + JSON.toJSONString(consumerQueueData));
            mappedByteBuffer.force();

        }
        System.out.println("ConsumerQueue数据保存完成count:" + count);


    }


    public static void createIndexFile() throws IOException {
        System.out.println("");
        System.out.println("IndexFile file create!" );

        //文件场创建时间，在写第一条消息的时候创建
        FileChannel fileChannel = FileChannel.open(Paths.get(URI.create("file:/c:/123/index.txt")),
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 409600);
        ByteBuffer headerByteBuffer = mappedByteBuffer.slice();
        long firstDataTime = System.currentTimeMillis();

        fileChannel.close();

        //开始写hash槽，从头部后写入
        /*  已经填充有index的slot数量
          （并不是每个slot槽下都挂载有index索引单元，这 里统计的是所有挂载了index索引单元的slot槽的数量，hash冲突）*/
        int hashSlotCount = 0;

        /* 已该indexFile中包含的索引单元个数（统计出当前indexFile中所有slot槽下挂载的所有index索引单元的数量之和），
        如果没有hash冲突,hashSlotCount = indexCount*/
        int indexCount = 0;
        //假设建立100个槽位（总长度400）
        int soltNum = 100;

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            IndexFileItemData indexFileItemData = indexFileItemDatas.get(i);
            int keyHash = indexFileItemData.getKeyHash();

            //取模，计算第几个槽位
            int slotPos = keyHash % 100 > 0 ? keyHash % 100 : -1 * (keyHash % 100);

            // slot存放的文件偏移量（字节长度）
            int absSlotPos = 40 + slotPos * 4;

            // 存储实际数据的文件偏移量（字节长度）
            int absIndexPos =
                    40 + soltNum * 4
                            + indexCount * 20;


            //将indexCount存到对应的hash槽
            mappedByteBuffer.putInt(absSlotPos, indexCount);

            //写入数据(IndecFile的实际数据部分)
            mappedByteBuffer.putInt(absIndexPos, indexFileItemData.getKeyHash());//8byte msg hashcode
            mappedByteBuffer.putLong(absIndexPos + 4, indexFileItemData.getPhyOffset());//8byte msg hashcode
            mappedByteBuffer.putInt(absIndexPos + 4 + 8, Integer.valueOf((System.currentTimeMillis() - firstDataTime) + ""));//8byte (timeDiff)
            mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, 0);//8byte (preIndex)暂不考虑hash冲突的情况


            //模拟最后一个文件，写入header
            if (i == 0) {
                //该indexFile中第一条消息的存储时间
                headerByteBuffer.putLong(0, firstDataTime);
                //该indexFile种第一条消息在commitlog种的偏移量commitlog offset
                mappedByteBuffer.putLong(16, indexFileItemData.getPhyOffset());
            }
            //模拟最后一个文件，写入header
            if (i == MESSAGE_COUNT - 1) {
                //该indexFile种最后一条消息存储时间
                headerByteBuffer.putLong(8, System.currentTimeMillis());
                //该indexFile中最后一条消息在commitlog中的偏移量commitlog offset
                headerByteBuffer.putLong(24, indexFileItemData.getPhyOffset());
            }
            //已经填充有index的slot数量
            headerByteBuffer.putInt(32, hashSlotCount + 1);
            //该indexFile中包含的索引单元个数
            headerByteBuffer.putInt(36, indexCount + 1);
            mappedByteBuffer.force();
            System.out.println("msgId:" + indexFileItemData.getMessageId() + ",keyHash:" + keyHash + ",保存槽位为" + slotPos + "的数据,absSlotPos=" + absSlotPos + ",值index=" + indexCount + ",绝对位置:" + absIndexPos + ",commit-phyOffset:" + indexFileItemData.getPhyOffset());

            indexCount++;
            hashSlotCount++;


        }

    }


    //将变长字符串定长byte[]，方便读取
    private static byte[] getBytes(String s, int length) {
        int fixLength = length - s.getBytes().length;
        if (s.getBytes().length < length) {
            byte[] S_bytes = new byte[length];
            System.arraycopy(s.getBytes(), 0, S_bytes, 0, s.getBytes().length);
            for (int x = length - fixLength; x < length; x++) {
                S_bytes[x] = 0x00;
            }
            return S_bytes;
        }
        return s.getBytes(StandardCharsets.UTF_8);
    }

}
