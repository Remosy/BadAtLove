import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//
//  Assign2.java
//  Assignment2
//
//  Created by RemosyXu on 27/4/19.
//   u6325688
//  Copyright © 2019 RemosyXu. All rights reserved.
//
public class Assign2 {


    private static final int PORT = 7880;
    private static final String HOST = "comp3310.ddns.net";
    private static final String CRLF = "\r\n";
    private static final String URLREGEX = "<a href=.*?>";
    private static final String URLREGEX2 = "<img src=.*?>";
    private static final String URLREGEX3 = "/.*?/";

    static Socket sock;

    //URLs
    static Queue<String> frontier; //Crawl Page by BFS
    static ArrayList<String> visited; //Crawl Page by BFS
    static ArrayList<String> validURL; //page with 200
    static ArrayList<String> invalidURL;//page with 404
    static HashMap<String,String> redirectedURL; //Key:page with 30X Value:page refer to redirectedURL

    //Size
    static String smallestPage;
    static int smallestSize = Integer.MAX_VALUE;
    static String largestPage;
    static int largestSize = 0;

    //Modification
    static String olderstPage;
    static Date olderstTimestamp;
    static String newestPage;
    static Date newestTimestamp;

    //HTML
    static int numHTMLPage = 0;
    static int numNONHTMLObj = 0;

    //Next indicator
    static Boolean ifGoNext = true;


    static void ASSconn() throws IOException {
        sock = new Socket(HOST, PORT);
        System.out.println("client: created socket connected to local port " +
                sock.getLocalPort() + " and to remote address " +
                sock.getInetAddress() + " and port " +
                sock.getPort());
    }

    static boolean checkStatus(String line, String url){
        String code = line.substring(9,12);
        switch (code){
            case "200":
                validURL.add(url);
                break;
            case "404":
                invalidURL.add(url);
                return false;
        }
        if(code.substring(0,2).contains("30")){
            return false;
        }
        return true;
    }

    int numDisctURL(){
        return visited.size();
    }


    int numHTML(){
        return 0;
    }

    /**
     * The smallest and largest html pages, and their sizes
     */
    static void checkSize(int size, String url){
        //Check Min
        if(size<smallestSize){
            smallestSize = size;
            smallestPage = url;
        }
        //Check Max
        if(size>largestSize){
            largestSize = size;
            largestPage = url;
        }

    }

    /**
     * The oldest and the most-recently modified page, and their date/timestamps
     */

    static void checkDate(String data, String url) throws ParseException {
        //String[] arr = data.split(" ", 0);//Thu, 11 Apr 2019 11:48:25 GMT
        SimpleDateFormat format = new SimpleDateFormat("dd MMMM yyyy HH:mm:ss");
        String[] d = data.split(" ");
        format.setTimeZone(TimeZone.getTimeZone(d[5]));
        String sdate = data.substring(5,data.length());
        Date date=format.parse(sdate);
        //Check old date
        if(date.compareTo(olderstTimestamp)<0){
            olderstTimestamp = date;
            olderstPage = url;
        }
        //Check new date
        if(date.compareTo(newestTimestamp)>0){
            newestTimestamp = date;
            newestPage = url;
        }

    }

    /**
     * The number of html pages & The number of non-html objects on the site(e.g. images)
     */
    static boolean checkIfHtml(String line){
        if(line.contains("html")){
            numHTMLPage ++;
            return true;
        }else{
            numNONHTMLObj ++;
            return false;
        }
    }

    static void checkContex(String icontex, String url){
    Pattern r = Pattern.compile(URLREGEX);
    Pattern r2 = Pattern.compile(URLREGEX2);
    Matcher m = r.matcher(icontex);
    Matcher m2 = r2.matcher(icontex);
    ArrayList<String> urls = new ArrayList<>();
    int i = 0;
    while (m.find()) {
        String tmp = m.group(0);//<a href="k/200.html">
        urls.add(tmp.substring(9,tmp.length()-2));
    }
    while (m2.find()) {
        String tmp = m2.group(0);//<img src="redback.jpg"> http://comp3310.ddns.net:7880/k/redback.jpg
        Pattern r3 = Pattern.compile(URLREGEX3);
        Matcher m3 = r3.matcher(url);
        if(m3.find()){
            String sub = m3.group(0);
            tmp = sub + tmp.substring(10,tmp.length()-2);
        }
        urls.add(tmp);
    }
    //If URL is not visited, put it to frontier
    for(String x:urls){
        if(!visited.contains(x)){
            //on the same site
            if(x.contains("http://comp3310.ddns.net:7880")){
                x = x.replace("http://comp3310.ddns.net:7880","");
            }

            //on the same site/not
            if (x.charAt(0)!='/' && !x.contains("http:")){
                x = "/"+x;
                System.out.println("Find Links: "+ x);
            }
            frontier.add(x);
        }
    }


    }


    /**
     Multiple-times SEND
     **/
    static void send_n(Socket isocket, String in) throws IOException {
        DataOutputStream out = new DataOutputStream(sock.getOutputStream());
        out.writeUTF(in);
        //str.append(in);
        System.out.println("[REQUEST SENT]");
    }

    /**
     Multiple-times RECV
     **/
    static void recv_n(Socket isocket, String url) throws IOException, ParseException {
        //DataInputStream in = new DataInputStream(sock.getInputStream());
        BufferedReader rd = new BufferedReader(new InputStreamReader(sock.getInputStream(), "utf-8"));
        StringBuilder sb = new StringBuilder();
        String line;
        int cforHeader = 0;
        String HTTPctx = "";
        String HTTPheader = "";
        boolean isvalid = true;
        boolean isHtml = true;
        //Size
        String ismallestPage = smallestPage;
        int ismallestSize = smallestSize;
        String ilargestPage = largestPage;
        int ilargestSize = largestSize;
        //Modification
        String iolderstPage = olderstPage;
        Date iolderstTimestamp = olderstTimestamp;
        String inewestPage = newestPage;
        Date inewestTimestamp = newestTimestamp;

        //System.out.println("[CHECKING HTTP HEADER...]");
        while ((line = rd.readLine()) != null) {
            System.out.println(line);
            if (line.contains("<html>")) {
                cforHeader = 1;
            }
            if (cforHeader == 0 && !line.equals("")) {
                if (line.contains("HTTP/")) {
                    isvalid = checkStatus(line, url);
                } else if (isvalid && line.substring(0, 14).contains("Content-Length")) {
                    checkSize(Integer.parseInt(line.substring(16, line.length())), url);
                } else if (line.substring(0, 13).contains("Last-Modified")) {
                    checkDate(line.substring(15, line.length()), url);
                } else if (isvalid && line.substring(0, 12).contains("Content-Type")) {
                    isHtml = checkIfHtml(line.substring(14, line.length()));
                }else if(!isvalid && line.substring(0, 9).contains("Location")){
                    redirectedURL.put(url,line.substring(10,line.length()).trim());
                }
            } else {
                if(!isHtml)break;//Don't read img data
                HTTPctx += line;
            }
        }
        if(!isHtml){
            //Size
            smallestPage = ismallestPage;
            smallestSize = ismallestSize;
            largestPage = ilargestPage;
            largestSize = ilargestSize;
            //Modification
            olderstPage = iolderstPage;
            olderstTimestamp = iolderstTimestamp;
            newestPage = inewestPage;
            newestTimestamp = inewestTimestamp;
        }
        rd.close();
        //System.out.println("[CHECKING HTTP CONTEXT...]");
        checkContex(HTTPctx,url);
        ifGoNext = true;
    }

    static void requestServer(String url) throws IOException, ParseException {
        //GET / HTTP/1.0
        //Conon:necti Keep-Alive
        // Host: comp3310.ddns.net:7880
        //Accept:*/*
        BufferedWriter msg = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream(), "utf-8"));
        msg.write("GET "+url+" HTTP/1.0"+CRLF);
        msg.write("HOST:" + HOST + ':' + PORT + "\r\n");
        msg.write("Accept:*/*"+CRLF);
        msg.write(CRLF);
        msg.flush();

       // BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream(), "utf-8"));
        //System.out.println("" +msg.toString());
        recv_n(sock,url);
    }

    public static void main (String[] args) throws InterruptedException, ParseException, IOException {
        /*---------------INIT VARIABLES-----------------------*/
        frontier = new LinkedList<String>();
        visited = new ArrayList<>();
        validURL = new ArrayList<>();
        invalidURL = new ArrayList<>();
        redirectedURL = new HashMap<>();
        olderstTimestamp = new Date(); //Today
        //String[] arr = data.split(" ", 0);//Thu, 11 Apr 2019 11:48:25 GMT
        SimpleDateFormat format = new SimpleDateFormat("dd MMMM yyyy HH:mm:ss z");
        String sdate = "11 Apr 1900 11:48:25 GMT";
        newestTimestamp = format.parse(sdate);

        /*---------------USE BFS-ish to traversal-----------------------*/
        frontier.add("/");//Init with root "/"
        while (!frontier.isEmpty() && ifGoNext == true) {
            ifGoNext = false;
            System.out.println("[URL]" + frontier.size() + " " + frontier.peek());
            String currentURL = frontier.poll();
            ASSconn();
            requestServer(currentURL);
            sock.close();
            visited.add(currentURL);
            System.out.println("\033[31m -------------------------------");
            System.out.println("\033[0m");
        }

        /*---------------Assignment Report-----------------------*/
        System.out.println("\033[31m Assignment 2 Report");
        System.out.println("\033[0m");

        String item1 = "Item1:  " + visited.size() +"\n";
        String item2 = "Item2:\n   [html pages]:" + numHTMLPage
                + "\n   [non-html object]:" + numNONHTMLObj + "\n";
        String item3 = "Item3:\n   Smallest:["+smallestPage+"]:"+
                smallestSize+"\n   Largest:["
                +largestPage+"]:"
                +largestSize + "\n";
        String item4 = "Item4:\n   Oldest:["+olderstPage+"]:"
                +olderstTimestamp.toString()
                +"\n   Most-recent:["+newestPage+"]:"+newestTimestamp+ "\n";

        System.out.println(item1+"\n");
        System.out.println(item2+"\n");
        System.out.println(item3+"\n");
        System.out.println(item4+"\n");

        String item5 = "Item5:";
        System.out.println(item5);
        int ct1 = 0;
        for(String x: invalidURL){
            ct1++;
            System.out.println("["+ct1+"] "+x);
        }
        System.out.println("\n");

        String item6 = "Item6:";
        System.out.println(item6);
        int width = 0;
        int ct2 = 0;
        for (String x:redirectedURL.keySet()){
            int s = x.length()+redirectedURL.get(x).length();
            if(s >width)width=s+8;
        }
        for (String k:redirectedURL.keySet()){
            ct2++;
            System.out.print("+");
            for(int ii = 0; ii<width; ii++){
                System.out.print("-");
            }
            System.out.print("+\n");

            System.out.print("|"+ct2+"|");
            System.out.print(k+" ===> "+redirectedURL.get(k)+"\n");

        }
        System.out.print("+");
        for(int jj = 0; jj<width; jj++){
            System.out.print("-");
        }
        System.out.print("+\n");



    }
}
