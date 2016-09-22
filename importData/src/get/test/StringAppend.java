package get.test;

import org.junit.Test;

public class StringAppend {
	@Test
	public void test01(){
		StringBuffer strbuf = new StringBuffer();
		strbuf.append("@\n");
		strbuf.append("@ACION:A\n");
		String value = strbuf.toString();
		System.out.println(value);
	}
}
