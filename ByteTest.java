package org.apache.rocketmq.demo;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ByteTest {


    /**
     * int到byte[]
     * @param i
     * @return
     */
    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        // 由高位到低位
        result[0] = (byte) ((i >> 24) & 0xFF);
        result[1] = (byte) ((i >> 16) & 0xFF);
        result[2] = (byte) ((i >> 8) & 0xFF);
        result[3] = (byte) (i & 0xFF);
        return result;
    }

    /**
     * byte[]转int
     * @param bytes
     * @return
     */
    public static int byteArrayToInt(byte[] bytes) {
        int value = 0;
        // 由高位到低位
        for (int i = 0; i < 4; i++) {
            int shift = (4 - 1 - i) * 8;
            value += (bytes[i] & 0x000000FF) << shift;// 往高位游
        }
        return value;
    }

    //测试数据
    public static void main(String[] args) {
        byte[] b = intToByteArray(128);
        System.out.println(Arrays.toString(b));//打印byte的每一个字节

        int i = byteArrayToInt(b);
        System.out.println(i);  //打印byte转变为Int后的数据

        ByteBuffer m = ByteBuffer.allocate(20);
        m.putInt(128);
        m.rewind();

        byte[] b1 = new byte[5];
        m.position(3);

        m.get(b1,1,4);//打印byte的每一个字节

        System.out.println(Arrays.toString(b1));//打印byte的每一个字节

    }


}
