package get.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TrsUtil {
	public static String date2Long(String dates) throws ParseException{
		//2009-01-01 12:30:30 //年-月-日
		//String reg1 = "^\\d{4}0[1-9]|1[1-2]0[1-9]|[1-2]\\d|3[0-1]$";
		String reg = "^\\d{4}$";
		Pattern pattern = Pattern.compile(reg);
		Matcher m=pattern.matcher(dates);
		SimpleDateFormat simpleDateFormat = null;
		if(m.find()){
			simpleDateFormat = new SimpleDateFormat("yyyy");
		}else if(dates == "0"){
			return null;
		}else{
			simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd");
		}
		Date date = null;
		Long time = null;
		
		if(dates.equals("")){
			return null;
		} else
		date = simpleDateFormat.parse(dates);
		time = date.getTime();
		Long timeStemp = time / 1000;
		return timeStemp.toString();
	}
	
	public static String date2Longs(String dates) throws ParseException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYYMMdd");
		Date date = null;
		Long time = null;
		date = simpleDateFormat.parse(dates);
		time = date.getTime();
		Long timeStemp = time / 1000;
		return timeStemp.toString();
	}
	
	public static String url2MD5(String url){
		return null;
	}
}
