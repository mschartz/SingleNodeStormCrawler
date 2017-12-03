package edu.upenn.cis.cis455.crawler;
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import edu.upenn.cis.cis455.crawler.info.URLInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/* HTTP Client for the web crawler */
public class HttpClient {
    private String hostname;
    private String path;
    private HashMap<String,String> headers;
    private int PORT;
    private final String USER_AGENT = "cis455crawler";
    static final Logger logger = LogManager.getLogger(HttpClient.class);
    private StringBuilder req;

    public HttpClient()
    {

    }

    public synchronized String sendRequest(URLInfo urlInfo, String requestType)
    {
        PORT = 80;
        String result = "";
        try {

                Socket socket = new Socket(urlInfo.getHostName(), PORT);

                InputStream in = socket.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                PrintWriter out = new PrintWriter(socket.getOutputStream());
                req = new StringBuilder();
                req.append(requestType + " ");
                req.append(urlInfo.getFilePath());
                req.append(" HTTP/1.1\r\n");
                req.append("Host: ");
 //               logger.debug("HOSTNAME IN CLIENT");
  //              logger.debug(urlInfo.getHostName());
                req.append(urlInfo.getHostName());
                req.append(":80 \r\n");
                req.append("Connection: close\r\n");
                req.append("\r\n");

                out.write(req.toString());

                String line;
                out.flush();
                logger.debug("reading response...");
                while ((line = reader.readLine()) != null) {
                  //  logger.debug(line);
                    result += (line + "\r\n");
                }
                 logger.debug("DB PAGE COUNT: " +  Crawler.DB_PAGE_COUNT);

                reader.close();
                socket.close();

        } catch (UnknownHostException e) {
            logger.debug("Unknown Host Error: " + e);
        } catch (IOException e) {
            logger.debug("IO Error: " + e);
        }

        return result;
    }

    public void head(URLInfo urlInfo) {

    }

    public void setHeader(String header, String val) {
        this.headers.put(header,val);
    }

}
