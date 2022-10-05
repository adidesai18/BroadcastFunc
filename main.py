from fastapi import FastAPI, Request,BackgroundTasks
import requests
from datetime import datetime
import os
from time import sleep
import firebase_admin
from firebase_admin import firestore
from zoneinfo import ZoneInfo
from firebase_admin import db
# import asyncio
# import aiohttp

app = FastAPI()
url = "https://api.ultramsg.com/instance14131/messages/chat"
cred_obj = firebase_admin.credentials.Certificate('firebase_credentials.json')
default_app = firebase_admin.initialize_app(cred_obj, {'databaseURL':"https://easysenddemo-default-rtdb.asia-southeast1.firebasedatabase.app/"})
DataBase = firestore.client()
RtdbRef = db.reference()
parameters=["receiver","message","attachments"]
ultramsg_headers = {'content-type': 'application/x-www-form-urlencoded'}


def group_List():
        groups_list=[]
        url = "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/contacts"
        querystring = {"token":os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")}
        response = requests.request("GET", url, headers=ultramsg_headers, params=querystring)
        for i in response.json():
                if i["isGroup"]==True:
                        groups_list.append(i["id"])
        return group_List

def start_broadcast(firestore_ref,rtdb_ref,broadcast_name,sender_list,json_data,broadcast_number,decode_msg):
        firestore_ref.set({
                "broadcast_name": broadcast_name,
                "created_at": datetime.now(tz=ZoneInfo('Asia/Kolkata'))
                })
        rtdb_ref.child(f"Broadcast/{firestore_ref.id}").update({
                "total_receiver":len(sender_list),
                "is_task_finished":False,
                "send_count":0,
        })
        for Receiver in sender_list:
                if json_data["message"]!="":
                        payload = "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+Receiver+",&body="+decode_msg
                        response = requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/chat", data=payload, headers=ultramsg_headers)
                        if response.text[2]=="e" :
                                        return {"error":response.json()["error"]}
                for attachment in json_data["attachments"]:
                        if attachment["send_as"]=="FileExtensionType.image":
                                img_payload= "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+Receiver+",&image="+attachment["URL"]
                                response=requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/image", headers=ultramsg_headers,data=img_payload)
                        elif attachment["send_as"]=="FileExtensionType.video":
                                video_payload = "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+Receiver+",&video="+attachment["URL"]
                                response=requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/video", data=video_payload, headers=ultramsg_headers)
                        else:
                                payload = "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+Receiver+",&filename="+attachment["file_name"]+"&document="+attachment["URL"]
                                response=requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/document", data=payload, headers=ultramsg_headers)
                        if response.text[2]=="e" :
                                return {"error":response.json()["error"]}
                broadcast_number+=1
                rtdb_ref.child(f"Broadcast/{firestore_ref.id}").update({"send_count":broadcast_number})
                print("send time "+str(datetime.now()))
                sleep(5)
                print("Final time "+str(datetime.now()))
        rtdb_ref.child(f"Broadcast/{firestore_ref.id}").update({"is_task_finished":True})

def broadcast(json_data):
        DecodeMessage=json_data["message"].encode('utf-8').decode("latin-1")
        FirestoreRef=DataBase.collection("products").document()
        BroadcastNumber=0
        if json_data and all(k in json_data for k in parameters):
                if json_data["onlyGroups"]=="true":
                        start_broadcast(firestore_ref=FirestoreRef,rtdb_ref=RtdbRef,broadcast_name="Groups",sender_list=group_List(),json_data=json_data,broadcast_number=BroadcastNumber,decode_msg=DecodeMessage)
                else:
                        BroadcastName=",".join(json_data["receiver"])
                        start_broadcast(firestore_ref=FirestoreRef,rtdb_ref=RtdbRef,broadcast_name=BroadcastName,sender_list=json_data["receiver"],json_data=json_data,broadcast_number=BroadcastNumber,decode_msg=DecodeMessage)
        else:
                return {"error":"Missing parameters(receiver,message,attachments)"}

@app.get("/healthz")
async def root():
    return "ok"

@app.post("/webhook")
async def get_body(request: Request):
    data=await request.json()
    print(data)
    return ""

@app.post("/")
async def get_body(background_task:BackgroundTasks,request: Request):
        JsonData=await request.json()
        print("Starting time "+str(datetime.now()))
        background_task.add_task(broadcast,JsonData)
        return "Message Sent"
