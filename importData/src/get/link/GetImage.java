package get.link;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class GetImage {

	/**
	 * 获取图片字节
	 * 
	 * @param urlString
	 * @return
	 * @throws Exception
	 */
	public static byte[] imageBytes(String urlString) throws Exception {
		byte[] tagInfo = null;
		if (!urlString.isEmpty()) {
			URL location = new URL(urlString);
			InputStream in = location.openStream();
			tagInfo = toByteArray(in);
			// byteimage(tagInfo, names);
		}
		return tagInfo;

	}

	public static byte[] toByteArray(InputStream in) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024 * 4];
		int n = 0;
		while ((n = in.read(buffer)) != -1) {
			out.write(buffer, 0, n);
		}
		return out.toByteArray();
	}
}
