package neu.mr.cs6240.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

/**
 * @author ajay subramanya & smitha bangalore naresh
 * @date 02/05/2016
 * @info Assignment 3 : General file utility methods for reading GZip files and
 *       printing mCv
 */
public class FileUtility {

	/**
	 * Reads the file gzip file and returns handler
	 *
	 * @param infile
	 * @return BufferedReader object to given infile
	 */
	public static BufferedReader readGZipFile(File infile) {
		FileInputStream fis = null;
		GZIPInputStream gis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		try {
			fis = new FileInputStream(infile);
			gis = new GZIPInputStream(fis);
			isr = new InputStreamReader(gis);
			return new BufferedReader(isr);
		} catch (java.util.zip.ZipException e) {
			System.out.println(infile.getName() + " Not in GZIP format");
		} catch (IOException e1) {
			System.err.println(e1.getMessage());
		}
		return br;
	}

	/**
	 * Creates a directory named dirName
	 */
	public static void createDirectory(String dirName) {
		File newDir = new File(dirName);
		if (!newDir.exists()) {
			newDir.mkdir();
		}
	}

	/**
	 * Takes res and resFileName to print m C p pair to that fileName
	 *
	 * @param res
	 *            Result ArrayList of AirlineMonthPrice
	 * @param resFileName
	 *            Results m C p are stored in this file name file name is
	 *            timestamped and also a directory is created.
	 *
	 */
	public static void printhmMonthPrice(ArrayList<AirlineMonthPrice> res, String resFileName,
			HashMap<String, Boolean> hmIsActive2015) {
		createDirectory(resFileName);
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new File(resFileName + "/" + resFileName + new Date().getTime() + ".csv"));
			for (AirlineMonthPrice amp : res) {
				String airline = amp.getAirline();
				if (hmIsActive2015.containsKey(airline) && hmIsActive2015.get(airline)) {
					StringBuilder sbCSV = new StringBuilder();
					DecimalFormat dm = new DecimalFormat("##.##");
					sbCSV.append(amp.getMonth() + "," + airline + "," + dm.format(amp.getPrice()));
					pw.println(sbCSV.toString());
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (pw != null) {
				pw.close();
			}
		}

	}

	/**
	 * appends time stamp to the the file name, so that while running the job
	 * multiple times we don't error out
	 *
	 * @param a
	 *            String to which we append the time stamp
	 * @return String that has the time stamp appended to it
	 */
	public static String appendTimeStampTo(String a) {
		return a + new SimpleDateFormat("yyyyMMddhhmm").format(new Date());
	}
}
