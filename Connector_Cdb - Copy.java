package chat_group;

import java.io.*;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class Connector_Cdb extends Thread{
    private int user_id=-1;
    private String login=null;
    ClientChat client;
    private HashSet<String> topicSet=new HashSet<>();

    private HashSet<String> getTopics(){
        return this.topicSet;
    }

    public Connector_Cdb(ClientChat client)
    {
       this.client=client;
    }
    public void run(){
        try{
            this.handleClient();
        } catch (IOException e) {
            try {
               // handleLogoff();
            } catch (Exception x) {
                 e.printStackTrace();
            }
            // e.printStackTrace();
        }
    }

    private void handleClient() throws IOException{
        //InputStream inputStream=this.client.getInputStream();
      //  this.outputStream=this.clientSocket.getOutputStream();

      //  BufferedReader reader=new BufferedReader(new InputStreamReader(inputStream));
        String line;

        while(( line=reader.readLine() )!=null){

            String[] tokens=line.split(" ");
            if(tokens==null || tokens.length <= 0){continue;}

            String cmd=tokens[0];

            if("logoff".equalsIgnoreCase(cmd) || "quit".equalsIgnoreCase(cmd)){
               // handleLogoff();
                break;
            }else if("login".equalsIgnoreCase(cmd)){
                this.handleLogin(outputStream,tokens);
            }else if("register".equalsIgnoreCase(cmd)){
                String[] tokensMsg=line.split(" ",3);
                handleRegister(tokensMsg);
            }else if("msg".equalsIgnoreCase(cmd)){
                String[] tokensMsg=line.split(" ",3);
                handleMessage(tokensMsg);
            }else if("join".equalsIgnoreCase(cmd)){
                handleJoin(tokens);
            }else if("leave".equalsIgnoreCase(cmd)){
                handleLeave(tokens);
            }else if("getUsers".equalsIgnoreCase(cmd)){
                handleGetUsers();
            }else if("getTopics".equalsIgnoreCase(cmd)){
                handleGetTopics();
            }else{
                String msg="unknown "+ cmd+"\n";
                write(msg);
            }

        }

       // this.clientSocket.close();
    }

    private void handleGetTopics() throws IOException {
        List<Connector_Cdb> conn_List = client.get_connector_list();

        HashSet<String> topics,uniqueTopics = new HashSet<>();

        for(Connector_Cdb connector: conn_List){
            topics = connector.getTopics();

            for(String topic:topics){
                uniqueTopics.add(topic);
            }
        }

        String msg="Topics";

        for (String topic:uniqueTopics){
            msg=msg+" "+topic;
        }
        msg=msg+"\n";
        write(msg);
    }

    private void write(String msg) {
        System.out.println(msg);
    }

    private void handleGetUsers() throws IOException {
        String users="";
        if(login == null){
            this.send("error Must Login First\n");
        }

        List<Connector_Cdb> conn = client.get_connector_list();
        for(Connector_Cdb connector: conn){

            if(! connector.getLogin().equalsIgnoreCase(login)){

                if(connector.getLogin()!=null)
                    users=users+" "+connector.getLogin();
            }
        }
        this.send("Users"+users+"\n");
    }

    private void handleRegister(String[] tokens) throws IOException {
        String msg;
        if( tokens.length != 3){return;}

        String username=tokens[1];
        String password=tokens[2];

        if(this.exists(username)){

            msg="error username exists\n";
            this.write(msg);
            return;
        }

        int user_id=this.createUser(username,password);

        if(user_id==-1){
            msg="error register failure\n";
        }else{
            msg="ok register\n";
        }

        this.write(msg);

    }

    private int createUser(String username, String password) {
        dbOperations dbOp=new dbOperations();

        try {
            int user_id=dbOp.newAccount(username, password);
            return user_id;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private boolean exists(String username) {
        dbOperations dbOp=new dbOperations();
        try{
            List<Map<String, Object>> users = dbOp.getUsers();
            for(Map<String, Object> user: users){
                if(username.equalsIgnoreCase((String)user.get("username"))){
                    return true;
                }
            }
        } catch (SQLException e) {
            // e.printStackTrace();
        }

        return false;
    }

    private void handleLeave(String[] tokens) {
        if(tokens.length <= 1){return;}

        String topic=tokens[1];
        topicSet.remove(topic);
    }

    public boolean isMemberOfTopic(String topic)
    {
        return topicSet.contains(topic);
    }
    private void handleJoin(String[] tokens) {
        if(tokens.length <= 1){return;}

        String topic=tokens[1];
        topicSet.add(topic);

    }

    // format: "msg" "login" body
    // format: "msg" "#topic" body
    private void handleMessage(String[] tokens) throws IOException {
        String sendTo=tokens[1];
        String body=tokens[2];

        boolean isTopic=sendTo.charAt(0) == '#';


        List<Connector_Cdb> conn_List = client.get_connector_list();
        for(Connector_Cdb connector: conn_List){
            if(isTopic){
                if(connector.isMemberOfTopic(sendTo)){
                    connector.send("msg "+sendTo+":"+login+" "+body+"\n");
                }
                continue;
            }

            if(connector.getLogin().equalsIgnoreCase(sendTo)){
                connector.send("msg "+login+" "+body+"\n");
                break;
            }
        }
    }

   /* private void handleLogoff() throws IOException {
        server.removeWorker(this);
        String offlineMsg="offline "+login+"\n";
        List<ServerWorker> workerList = server.getWorkerList();

        // notify users on logoff user
        for(ServerWorker worker: workerList){
            if(!login.equals(worker.getLogin()))
            {
                worker.send(offlineMsg);

            }
        }
        this.clientSocket.close();
    }

    */

    public String getLogin(){
        return this.login;
    }


    private void handleLogin(OutputStream outputstream,String[] tokens) throws IOException{

        if(tokens.length ==3){
            String login=tokens[1];
            String password=tokens[2];

            if(
                    this.isAuthenticated(login,password)
            )
            {
                String msg="Ok login\n";
                outputstream.write(msg.getBytes());
                this.login=login;
                System.out.println("user logged in successfully: "+login);
                String onlineMsg="online "+login+"\n";
                List<Connector_Cdb> conn_List = client.get_connector_list();
                // notify user on all online users
                for(Connector_Cdb connector: conn_List){
                    if(!login.equals(connector.getLogin()))
                    {
                        String msg2 = "online " + connector.getLogin()+"\n";
                        if(connector.getLogin()!=null)
                            this.send(msg2);

                    }
                }

                //send all other online users user's status
                for(Connector_Cdb connector: conn_List){
                    if(connector.getLogin()!=null) {
                        if(!login.equals(connector.getLogin())) {
                            connector.send(onlineMsg);
                        }
                    }
                }
            }else{
                String msg="error login\n";
                outputstream.write(msg.getBytes());
            }
        }
    }

    private boolean isAuthenticated(String username,String password){
        dbOperations dbOp=new dbOperations();
        try {

            List<Map<String, Object>> users=dbOp.auth(username,password);
            if(users.size() == 1)
            {
                Map<String, Object> user=users.get(0);
                this.user_id=(int) user.get("user_id");
                return true;
            }

        } catch (SQLException e) {
            // e.printStackTrace();
        }

        return false;
    }

    private void send(String Msg) throws IOException {
        if(login !=null)
            this.write(Msg);
    }
}
