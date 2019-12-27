package chat_group;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ClientChat  extends Thread {

    public String username;
    private HashSet<String> topicSet=new HashSet<>();
    private  final String EXCHANGE_NAME = "topic_logs";

    private BufferedReader bufferIn;

    private String response;

    private Map<String, Set<String[]>> TopicMessages = new HashMap<String, Set<String[]>>();

    private ArrayList<MessageListener> messageListener=new ArrayList<>();
    private ArrayList<UserStatusListener> userStatusListeners=new ArrayList<>();
    private List<Connector_Cdb> clientList;

    public List<Connector_Cdb> get_connector_list(){
        return clientList;
    }

    public HashSet<String> getSubscribedTopics(){
        return this.topicSet;
    }

    Scanner reader;

    public Set<String[]> getMessagesFromTopic(String topic){
        Set<String[]> messageSet;
        if(!TopicMessages.containsKey(topic)){
            messageSet=new HashSet<>();
        }else{
            messageSet = TopicMessages.get(topic);
        }
        return messageSet;
    }

    public ClientChat()
    {
         reader=new Scanner(System.in);
    }

    public boolean login(String name,String password) throws IOException {
        String cmd="login "+name+" "+password+"\n";
        write(cmd);
        ///////////////////
        String response = bufferIn.readLine();
        System.out.println("Response : "+response);
        if(! "Ok login".equalsIgnoreCase(response)){
            return false;
        }

        startMessageReader();
        return true;
    }

    private void write(String msg) {
        System.out.println(msg);
    }

    public boolean register(String name,String password) throws IOException {
        String cmd="register "+name+" "+password+"\n";
        write(cmd);
        /////////////////////////////
        String response = bufferIn.readLine();
        System.out.println("Response : "+response);
        if(! "ok register".equalsIgnoreCase(response)){
            return false;
        }

        return true;
    }

    private void startMessageReader() {

        multi t=new multi(this);
        t.start();
    }

    public void connect()throws Exception
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                channel.exchangeDeclare(EXCHANGE_NAME, "topic");
           // this.bufferIn=new BufferedReader(new InputStreamReader(serverIn));

            String routingKey = getRouting(argv + "args of main method");
            String message = getMessage(argv + "args of main method");

            channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
        }
    }
    //..
    private static String getRouting(String[] strings) {
        if (strings.length < 1)
            return "anonymous.info";
        return strings[0];
    }

    private static String getMessage(String[] strings) {
        if (strings.length < 2)
            return "Hello World!";
        return joinStrings(strings, " ", 1);
    }

    private static String joinStrings(String[] strings, String delimiter, int startIndex) {
        int length = strings.length;
        if (length == 0) return "";
        if (length < startIndex) return "";
        StringBuilder words = new StringBuilder(strings[startIndex]);
        for (int i = startIndex + 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }


    public void readMessageLoop() throws IOException {
        String line;

        while((line=this.bufferIn.readLine())!=null)
        {
            this.response=line;

            String[] tokens=line.split(" ",3);
            if(tokens==null || tokens.length==0){ continue; }

            String cmd=tokens[0];

            if("online".equalsIgnoreCase(cmd)){
                handleOnline(tokens);
            } else if("offline".equalsIgnoreCase(cmd)){
                handleOffline(tokens);
            }else if("msg".equalsIgnoreCase(cmd)){
                handleMessage(tokens);
            }
        }

        //this.socket.close();
    }

    private void handleMessage(String[] tokens) {
        String login=tokens[1];
        String msBody=tokens[2];
        String[] topicUser=login.split(":");

        if(topicUser.length ==1){
            for(MessageListener listener: messageListener){
                listener.onMessage(login,msBody);
            }
            return;
        }

        String topic=topicUser[0].substring(1);
        String user=topicUser[1];

        String[] message=new String[2];
        message[0]=user;
        message[1]=msBody;

        Set<String[]> newSet;

        if(!TopicMessages.containsKey(topic)){

            newSet=new HashSet<>();
            newSet.add(message);
            TopicMessages.put(topic,newSet);
        }else{
            newSet = TopicMessages.get(topic);

            newSet.add(message);

            TopicMessages.replace(topic,newSet);
        }

        for(MessageListener listener: messageListener){
            listener.onMessage(user,msBody);
        }
    }

    public void sendMessage(String to,String message) throws IOException {
        String cmd="msg "+to+" "+message+"\n";
        write(cmd);
    }



    private void handleOffline(String[] tokens) {
        String login=tokens[1];

        for(UserStatusListener listener:userStatusListeners){
            listener.offline(login);
        }
    }

    public void addMessageListener(MessageListener messageListener){
        this.messageListener.add(messageListener);
    }
    public void removeMessageListener(MessageListener messageListener){
        this.messageListener.remove(messageListener);
    }

    private void handleOnline(String[] tokens) {
        String login=tokens[1];

        for(UserStatusListener listener:userStatusListeners){
            listener.online(login);
        }
    }

    public void joinTopic(String text) throws IOException {
        String cmd="join #"+text+"\n";

        write(cmd);

        topicSet.add(text);

    }

    public String[] getAllTopics() throws IOException {
        String cmd="getTopics\n";
        write(cmd);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String response = this.response;

        System.out.println("Response : "+response);
        String[] ss=response.split(" #");
        return ss;
    }

    public void leaveTopic(String text) throws IOException {
        String cmd="leave #"+text+"\n";

        write(cmd);

        topicSet.remove(text);

    }

    public void logoff() throws IOException {
        String cmd="logoff"+"\n";

        write(cmd);
    }
}

class multi extends Thread {

    ClientChat client;

    public multi(ClientChat client) {
        this.client = client;
    }

    public void run() {
        try {
            this.client.readMessageLoop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}