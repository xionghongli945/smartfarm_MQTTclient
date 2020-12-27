#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mosquitto.h"
#include "/usr/include/json-c/json.h"

#define DEBUG_PROCESS   printf
#define DEBUG_ERROR     printf
#define DEBUG_MSG       printf


// 运行标志决
static int Flag = 1;
static struct mosquitto *mosq_pub;

// 回调函数 
void on_connect(struct mosquitto *mosq, void *obj, int rc);
void on_disconnect(struct mosquitto *mosq, void *obj, int rc);
void on_subscribe(struct mosquitto *mosq, void *obj, int mid, int qos_count, const int *granted_qos);
void on_message(struct mosquitto *mosq_sub, void *obj, const struct mosquitto_message *msg);
void on_connect_pub(struct mosquitto *mosq, void *obj, int rc);
void on_publish(struct mosquitto *mosq, void *obj, int mid);
//json转换函数
char json_to_json(char *s);

int main()
{
        int ret;
        struct mosquitto *mosq_sub;

        // 初始化mosquitto库
        ret = mosquitto_lib_init();
        if(ret){
                DEBUG_ERROR("Init lib error!\n");
                return -1;
        }
        // 创建客户端
        mosq_sub =  mosquitto_new("New_sub", true, NULL);
        if(mosq_sub == NULL){
                DEBUG_ERROR("New sub error!\n");
                mosquitto_lib_cleanup();
                return -1;
        }
	mosq_pub =  mosquitto_new("New_pub", true, NULL);
        if(mosq_pub == NULL){
                DEBUG_ERROR("New pub error!\n");
                mosquitto_lib_cleanup();
                return -1;
        }
        // 设置回调函数
        mosquitto_connect_callback_set(mosq_sub, on_connect);
        mosquitto_disconnect_callback_set(mosq_sub, on_disconnect);
        mosquitto_subscribe_callback_set(mosq_sub, on_subscribe);
        mosquitto_message_callback_set(mosq_sub, on_message);

	mosquitto_connect_callback_set(mosq_pub, on_connect_pub);
	mosquitto_disconnect_callback_set(mosq_pub, on_disconnect);
	mosquitto_publish_callback_set(mosq_pub, on_publish);
	
        // 连接本地服务器（订阅）
        //ret = mosquitto_connect_async(mosq_sub,"localhost",1883, 60);
	ret = mosquitto_connect_async(mosq_sub,"118.24.19.135",1883, 60);
       	if(ret){
                DEBUG_ERROR("Connect sub server error!\n");
                mosquitto_destroy(mosq_sub);
                mosquitto_lib_cleanup();
                return -1;
        }

		ret = mosquitto_loop_start(mosq_sub);
        if(ret){
                DEBUG_ERROR("Start loop error!\n");
                mosquitto_destroy(mosq_sub);
                mosquitto_lib_cleanup();
                return -1;
        }
	//连接服务器（发布）
	ret = mosquitto_connect_async(mosq_pub,"118.24.19.135",1883, 60);
        if(ret){
                DEBUG_ERROR("Connect pub server error!\n");
                mosquitto_destroy(mosq_pub);
                mosquitto_lib_cleanup();
                return -1;
        }

                ret = mosquitto_loop_start(mosq_pub);
        if(ret){
                DEBUG_ERROR("Start loop error!\n");
                mosquitto_destroy(mosq_pub);
                mosquitto_lib_cleanup();
                return -1;
        }

        // 循环执行直到运行标志Flag被改变
        DEBUG_PROCESS("Process Start!\n");
       
       	while(Flag)
        {
                sleep(1);
                DEBUG_PROCESS("...\n");
        }
	
        // 清理工作
        mosquitto_destroy(mosq_sub);
	mosquitto_destroy(mosq_pub);
        mosquitto_lib_cleanup();
        DEBUG_PROCESS("End!\n");

        return 0;
}


void on_connect(struct mosquitto *mosq, void *obj, int rc)
{
        DEBUG_PROCESS("Call the function: on_connectS\n");

        if(rc){
                // 连接错误
                DEBUG_ERROR("on_connect_S error!\n");
                exit(1);
        }else{
                // 订阅主题
		// 实例、句柄、主题、qos
		//if(mosquitto_subscribe(mosq, NULL, "tick", 2)){
                if(mosquitto_subscribe(mosq, NULL, "test", 2)){
                        DEBUG_ERROR("Set the topic error!\n");
                        exit(1);
                }
        }
}

void on_disconnect(struct mosquitto *mosq, void *obj, int rc)
{
        DEBUG_PROCESS("Call the function: on_disconnect\n");

        Flag = 0;
}

void on_subscribe(struct mosquitto *mosq, void *obj, int mid, int qos_count, const int *granted_qos)
{
        DEBUG_PROCESS("Call the function: on_subscribe\n");
}

void on_message(struct mosquitto *mosq_sub, void *obj, const struct mosquitto_message *msg)
{
        DEBUG_PROCESS("Call the function: on_message\n");
        DEBUG_MSG("Recieve a message: %s\n", (char *)msg->payload);
	/*
        if(0 == strcmp(msg->payload, "quit")){
                mosquitto_disconnect(mosq_sub);
        }
	*/
	//调用json转换函数并推送到服务器
	json_to_json((char *)msg->payload);
}
void on_connect_pub(struct mosquitto *mosq, void *obj, int rc)
{
	DEBUG_PROCESS("Call the function:on_connect_pub\n");
}
void on_publish(struct mosquitto *mosq, void *obj, int mid)
{
	DEBUG_PROCESS("Call the function:on_publish\n");
}
char json_to_json(char *s)
{
	struct json_object *json_policy_array;
	json_policy_array = json_tokener_parse(s);
        if(NULL == json_policy_array)
        {
		DEBUG_ERROR("json_array created error!\n");
                return 0;
        }
	struct json_object *json_actioncode ;
	struct json_object *postdata;
	struct json_object *json_pub = json_object_new_object();
	json_actioncode = json_object_object_get(json_policy_array,"actioncode");	
	postdata = json_object_object_get(json_policy_array,"postdata");
	//温湿度传感器json数据
	if(!strcmp(json_object_to_json_string(json_actioncode),"\"temphumidityReturn\""))
        {
                struct json_object * sensorDevID = json_object_object_get(postdata,"sensorDevID");
                struct json_object * groupID = json_object_object_get(postdata,"groupID");
                struct json_object * temperature = json_object_object_get(postdata,"temperature");
		struct json_object * humidity = json_object_object_get(postdata,"humidity");
		struct json_object * timestamp = json_object_object_get(postdata,"timestamp");
                struct json_object * voletage = json_object_object_get(postdata,"voletage");
                //构建发布到服务器json数据
                json_object_object_add(json_pub,"messageclass",json_object_new_string("sensorData"));
                json_object_object_add(json_pub,"deviceID",sensorDevID);
                json_object_object_add(json_pub,"typeID",groupID);
                json_object_object_add(json_pub,"temperature",temperature);
		json_object_object_add(json_pub,"humidity",humidity);
                json_object_object_add(json_pub,"voletage",voletage);
                json_object_object_add(json_pub,"timestamp",timestamp);
        }
	//光照传感器json数据
	else if(!strcmp(json_object_to_json_string(json_actioncode),"\"illuminanceReturn\""))
        {
                struct json_object * sensorDevID = json_object_object_get(postdata,"sensorDevID");
                struct json_object * groupID = json_object_object_get(postdata,"groupID");
                struct json_object * illuminance = json_object_object_get(postdata,"illuminance");
		struct json_object * timestamp = json_object_object_get(postdata,"timestamp");
                struct json_object * voletage = json_object_object_get(postdata,"voletage");
                //构建发布到服务器json数据
                json_object_object_add(json_pub,"messageclass",json_object_new_string("sensorData"));
                json_object_object_add(json_pub,"deviceID",sensorDevID);
                json_object_object_add(json_pub,"typeID",groupID);
                json_object_object_add(json_pub,"illuminance",illuminance);
                json_object_object_add(json_pub,"voletage",voletage);
                json_object_object_add(json_pub,"timestamp",timestamp);
        }
	//PM2.5传感器json数据
	else if(!strcmp(json_object_to_json_string(json_actioncode),"\"pm2dot5Return\""))
        {
                struct json_object * sensorDevID = json_object_object_get(postdata,"sensorDevID");
                struct json_object * groupID = json_object_object_get(postdata,"groupID");
                struct json_object * pm2dot5 = json_object_object_get(postdata,"pm2dot5");
		struct json_object * timestamp = json_object_object_get(postdata,"timestamp");
                struct json_object * voletage = json_object_object_get(postdata,"voletage");
                //构建发布到服务器json数据
                json_object_object_add(json_pub,"messageclass",json_object_new_string("sensorData"));
                json_object_object_add(json_pub,"deviceID",sensorDevID);
                json_object_object_add(json_pub,"typeID",groupID);
                json_object_object_add(json_pub,"pm2dot5",pm2dot5);
                json_object_object_add(json_pub,"voletage",voletage);
                json_object_object_add(json_pub,"timestamp",timestamp);
        }
	//土壤水分传感器json数据
	else if(!strcmp(json_object_to_json_string(json_actioncode),"\"moistureReturn\""))
        {
                struct json_object * sensorDevID = json_object_object_get(postdata,"sensorDevID");
                struct json_object * groupID = json_object_object_get(postdata,"groupID");
                struct json_object * moisture = json_object_object_get(postdata,"moisture");
		struct json_object * temperature = json_object_object_get(postdata,"temperature");
		struct json_object * timestamp = json_object_object_get(postdata,"timestamp");
                struct json_object * voletage = json_object_object_get(postdata,"voletage");
                //构建发布到服务器json数据
                json_object_object_add(json_pub,"messageclass",json_object_new_string("sensorData"));
                json_object_object_add(json_pub,"deviceID",sensorDevID);
                json_object_object_add(json_pub,"typeID",groupID);
                json_object_object_add(json_pub,"moisture",moisture);
		json_object_object_add(json_pub,"temperature",temperature);
                json_object_object_add(json_pub,"voletage",voletage);
                json_object_object_add(json_pub,"timestamp",timestamp);
        }
	//土壤酸碱度传感器json数据
	else if(!strcmp(json_object_to_json_string(json_actioncode),"\"soilPHReturn\""))
        {
                struct json_object * sensorDevID = json_object_object_get(postdata,"sensorDevID");
                struct json_object * groupID = json_object_object_get(postdata,"groupID");
                struct json_object * soilPH = json_object_object_get(postdata,"soilPH");
		struct json_object * timestamp = json_object_object_get(postdata,"timestamp");
                struct json_object * voletage = json_object_object_get(postdata,"voletage");
                //构建发布到服务器json数据
                json_object_object_add(json_pub,"messageclass",json_object_new_string("sensorData"));
                json_object_object_add(json_pub,"deviceID",sensorDevID);
                json_object_object_add(json_pub,"typeID",groupID);
                json_object_object_add(json_pub,"soilPH",soilPH);
                json_object_object_add(json_pub,"voletage",voletage);
                json_object_object_add(json_pub,"timestamp",timestamp);
        }
	//风速传感器json数据
	else if(!strcmp(json_object_to_json_string(json_actioncode),"\"windspeedReturn\""))
        {
                struct json_object * sensorDevID = json_object_object_get(postdata,"sensorDevID");
                struct json_object * groupID = json_object_object_get(postdata,"groupID");
                struct json_object * windspeed = json_object_object_get(postdata,"windspeed");
		struct json_object * timestamp = json_object_object_get(postdata,"timestamp");
                struct json_object * voletage = json_object_object_get(postdata,"voletage");
                //构建发布到服务器json数据
                json_object_object_add(json_pub,"messageclass",json_object_new_string("sensorData"));
                json_object_object_add(json_pub,"deviceID",sensorDevID);
                json_object_object_add(json_pub,"typeID",groupID);
                json_object_object_add(json_pub,"windspeed",windspeed);
                json_object_object_add(json_pub,"voletage",voletage);
                json_object_object_add(json_pub,"timestamp",timestamp);
        }
	//风向传感器json数据
	else if(!strcmp(json_object_to_json_string(json_actioncode),"\"winddirectionReturn\""))
        {
                struct json_object *sensorDevID = json_object_object_get(postdata,"sensorDevID");
                struct json_object *groupID = json_object_object_get(postdata,"groupID");
                struct json_object *winddirection = json_object_object_get(postdata,"winddirection");
		struct json_object *timestamp = json_object_object_get(postdata,"timestamp");
		struct json_object *voletage = json_object_object_get(postdata,"voletage");
		//构建发布到服务器json数据
		json_object_object_add(json_pub,"messageclass",json_object_new_string("sensorData"));
		json_object_object_add(json_pub,"deviceID",sensorDevID);
		json_object_object_add(json_pub,"typeID",groupID);
		json_object_object_add(json_pub,"winddirection",winddirection);
		json_object_object_add(json_pub,"voletage",voletage);
		json_object_object_add(json_pub,"timestamp",timestamp);
        }
	//雨量传感器json数据
	else if(!strcmp(json_object_to_json_string(json_actioncode),"\"rainsnowReturn\""))
        {
                struct json_object * sensorDevID = json_object_object_get(postdata,"sensorDevID");
                struct json_object * groupID = json_object_object_get(postdata,"groupID");
                struct json_object * rainsnow = json_object_object_get(postdata,"rainsnow");
		struct json_object * timestamp = json_object_object_get(postdata,"timestamp");
		struct json_object * voletage = json_object_object_get(postdata,"voletage");
		//构建发布到服务器json数据
                struct json_object * json_pub;
                json_object_object_add(json_pub,"messageclass",json_object_new_string("sensorData"));
                json_object_object_add(json_pub,"deviceID",sensorDevID);
                json_object_object_add(json_pub,"typeID",groupID);
                json_object_object_add(json_pub,"rainsnow",rainsnow);
                json_object_object_add(json_pub,"voletage",voletage);
                json_object_object_add(json_pub,"timestamp",timestamp);
        }
	else
		exit(1);
	//发布到MQTT服务器“jcsf/iotdata”主题
	//参数：实例、句柄、主题、有效负载长度、消息内容、qos、是否保留消息
	//printf("%s\n",json_object_to_json_string(json_pub));
	mosquitto_publish(mosq_pub,NULL,"jcsf/iotdata",strlen(json_object_to_json_string(json_pub)),(char *)json_object_to_json_string(json_pub),0,0);
}
