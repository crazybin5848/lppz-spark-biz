package com.lppz.spark.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;

import com.lppz.spark.util.FileNioUtil;

public class FileNioAppendUtil extends FileNioUtil {


	public static void writeWithMappedByteBuffer(String fileDir, String fileName, StringBuilder sb) throws IOException {
		File dir = new File(fileDir);
		if (!dir.exists())
			makeDir(dir);

		File file = new File((new StringBuilder()).append(fileDir).append("/").append(fileName != null ? fileName : UUID.randomUUID().toString()).append(".sql").toString());
		byte data[] = sb.toString().getBytes();
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel fileChannel = raf.getChannel();
		int pos = 0;
		MappedByteBuffer mbb = null;
		long len = data.length;
		int dataChunk = 64;
		while (len >= 67108864L) {
			System.out.println((new StringBuilder()).append("write a data chunk: ").append(dataChunk).append("MB").toString());
			mbb = fileChannel.map(java.nio.channels.FileChannel.MapMode.READ_WRITE, file.length(), 67108864L);
			mbb.put(data, pos, 67108864);
			len -= 67108864L;
			pos += 67108864;
		}
		if (len > 0L) {
			System.out.println((new StringBuilder()).append("write rest data chunk: ").append(len / 1048576L).append("MB").toString());
			mbb = fileChannel.map(java.nio.channels.FileChannel.MapMode.READ_WRITE, file.length(), len);
			mbb.put(data, pos, (int) len);
		}
		data = null;
		unmap(mbb);
		fileChannel.close();
		raf.close();
	}

}
