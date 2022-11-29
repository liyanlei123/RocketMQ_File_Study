package org.apache.rocketmq.demo;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

//8byte(commitlog offset)
//4byte （msgLength)
//8byte (tagCode)
public class FileWrite {
    public static void main(String[] args) throws IOException {

        FileChannel fileChannel = FileChannel.open(Paths.get(URI.create("file:/c:/123/111.txt")),
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
        fileChannel.close();

       for(int i =0;i<10;i++){
            mappedByteBuffer.position(i*20);
            ByteBuffer  b = ByteBuffer.allocate(20);
            b.putLong(100);//8byte(commitlog offset)
            b.putInt(1000);//4byte （msgLength)
            b.putLong(20);//8byte (tagCode)
            b.flip();
            mappedByteBuffer.put(b);

        }

        mappedByteBuffer.force();

    }
}
