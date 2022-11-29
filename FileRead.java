package org.apache.rocketmq.demo;

import java.io.IOException;
import java.net.URI;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileRead {
    public static void main(String[] args) throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(URI.create("file:/c:/123/111.txt")),
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
        fileChannel.close();

        for(int i =0;i<10;i++){
            mappedByteBuffer.position(i*20);
            long commitlogOffset = mappedByteBuffer.getLong();
            long msgLen = mappedByteBuffer.getInt();
            long tagCode = mappedByteBuffer.getLong();
            System.out.println("文件读取：commitlogOffset："+commitlogOffset+",msgLen:"+msgLen+",tagCode:"+tagCode);
        }
    }
}
