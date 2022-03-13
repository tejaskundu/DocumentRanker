package uk.ac.gla.dcs.bigdata.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class RealizationEngineClient {

	
	/**
	 * Instructs the Realization Engine to start the 'build' operation sequence for the
	 * student's application. This is the first in the chain of operation sequences that
	 * runs the students application remotely.
	 * @param teamid
	 * @return
	 */
	public static boolean startBuildOperationSequenceForTeam(String teamid) {

		try {
			URL url = new URL("http://gdtapi-compsci5088bigdataproject.ida.dcs.gla.ac.uk:80/api/v1/exe/compsci5088bigdata/"+teamid+"/build/start");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			con.setDoOutput(true);
			con.setRequestProperty("Content-Type", "application/json");
			con.setConnectTimeout(5000);
			con.setReadTimeout(5000);

			int status = con.getResponseCode();
			if (status == HttpURLConnection.HTTP_MOVED_TEMP
					|| status == HttpURLConnection.HTTP_MOVED_PERM) {
				String location = con.getHeaderField("Location");
				URL newUrl = new URL(location);
				con = (HttpURLConnection) newUrl.openConnection();
			}

			BufferedReader in = new BufferedReader(
					new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer content = new StringBuffer();
			while ((inputLine = in.readLine()) != null) {
				content.append(inputLine);
			}
			in.close();
			con.disconnect();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Instructs the Realization Engine to register the specified playbook for a team and project pair.
	 * @param teamid
	 * @param project
	 * @return
	 */
	public static boolean registerApplication(String teamid, String project, String playbook) {

		try {
			URL url = new URL("http://gdtapi-compsci5088bigdataproject.ida.dcs.gla.ac.uk:80/api/v1/registerfile/playbook/compsci5088bigdata/compsci5088bigdataproject/"+playbook+"/"+teamid+"/"+project);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			con.setDoOutput(true);
			con.setRequestProperty("Content-Type", "application/json");
			con.setConnectTimeout(5000);
			con.setReadTimeout(5000);

			int status = con.getResponseCode();
			if (status == HttpURLConnection.HTTP_MOVED_TEMP
					|| status == HttpURLConnection.HTTP_MOVED_PERM) {
				String location = con.getHeaderField("Location");
				URL newUrl = new URL(location);
				con = (HttpURLConnection) newUrl.openConnection();
			}

			BufferedReader in = new BufferedReader(
					new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer content = new StringBuffer();
			while ((inputLine = in.readLine()) != null) {
				content.append(inputLine);
			}
			in.close();
			con.disconnect();
			
			Thread.sleep(1000); // wait 1 second to be sure the database has updated
			
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
}
