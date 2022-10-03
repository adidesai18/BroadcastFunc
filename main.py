from fastapi import FastAPI, Request
import requests
from datetime import datetime
import os
from time import sleep
import firebase_admin
from firebase_admin import firestore
from zoneinfo import ZoneInfo
from firebase_admin import db

app = FastAPI()
url = "https://api.ultramsg.com/instance14131/messages/chat"
cred_obj = firebase_admin.credentials.Certificate('firebase_credentials.json')
default_app = firebase_admin.initialize_app(cred_obj, {'databaseURL':os.environ.get("RTDB_URL")})
dataBase = firestore.client()
ref_RTDB = db.reference()
parameters=["receiver","message","attachments"]
ultramsg_headers = {'content-type': 'application/x-www-form-urlencoded'}
secret_key=os.environ.get("ACCESS_TOKEN")


def group_List():
        groups_list=[]
        url = "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/contacts"
        querystring = {"token":os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")}
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        response = requests.request("GET", url, headers=headers, params=querystring)
        for i in response.json():
                if i["isGroup"]==True:
                        groups_list.append(i["id"])
        return group_List

def send_message(request_json,receiver,ref,decode_msg,broadcast_number):
        if request_json["message"]!="":
                payload = "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+receiver+",&body="+decode_msg
                try:
                        response = requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/chat", data=payload, headers=ultramsg_headers,timeout=0.5)
                        if response.text[2]=="e" :
                                return {"error":response.json()["error"]}
                except:
                        pass
        for attachment in request_json["attachments"]:
                try:
                        if attachment["send_as"]=="FileExtensionType.image":
                                img_payload= "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+receiver+",&image="+attachment["URL"]
                                response=requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/image", headers=ultramsg_headers,data=img_payload,timeout=0.5)
                        elif attachment["send_as"]=="FileExtensionType.video":
                                video_payload = "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+receiver+",&video="+attachment["URL"]
                                response=requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/video", data=video_payload, headers=ultramsg_headers,timeout=0.5)
                        else:
                                payload = "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+receiver+",&filename="+attachment["file_name"]+"&document="+attachment["URL"]
                                response=requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/document", data=payload, headers=ultramsg_headers,timeout=0.5)
                        if response.text[2]=="e" :
                                return {"error":response.json()["error"]}
                except:
                        pass
        broadcast_number+=1
        ref_RTDB.child(f"Broadcast/{ref.id}").update({"send_count":broadcast_number})
        sleep(5)

def broadcast(Json_data):
    if Json_data and all(k in Json_data for k in parameters):
        if Json_data["onlyGroups"]=="true":
                broadcast_number=0
                groups_list=group_List()
                decode_msg=Json_data["message"].encode('utf-8').decode("latin-1")
                ref=dataBase.collection("products").document()
                ref.set({
                        "broadcast_name": "Groups",
                        "created_at": datetime.now(tz=ZoneInfo('Asia/Kolkata'))
                        })
                ref_RTDB.child(f"Broadcast/{ref.id}").update({
                        "total_receiver":len(groups_list),
                        "is_task_finished":False,
                        "send_count":broadcast_number,
                })
                for receiver in groups_list:
                        if Json_data["message"]!="":
                                payload = "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+receiver+",&body="+decode_msg
                                response = requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/chat", data=payload, headers=ultramsg_headers)
                                if response.text[2]=="e" :
                                                return {"error":response.json()["error"]}
                        for attachment in Json_data["attachments"]:
                                if attachment["send_as"]=="FileExtensionType.image":
                                        img_payload= "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+receiver+",&image="+attachment["URL"]
                                        response=requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/image", headers=ultramsg_headers,data=img_payload)
                                elif attachment["send_as"]=="FileExtensionType.video":
                                        video_payload = "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+receiver+",&video="+attachment["URL"]
                                        response=requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/video", data=video_payload, headers=ultramsg_headers)
                                else:
                                        payload = "token="+os.environ.get("ULTRAMSG_WHATSAPP_TOKEN")+"&to="+receiver+",&filename="+attachment["file_name"]+"&document="+attachment["URL"]
                                        response=requests.request("POST", "https://api.ultramsg.com/"+os.environ.get("ULTRAMSG_INSTANCE_ID")+"/messages/document", data=payload, headers=ultramsg_headers)
                                if response.text[2]=="e" :
                                        return {"error":response.json()["error"]}
                        broadcast_number+=1
                        ref_RTDB.child(f"Broadcast/{ref.id}").update({"send_count":broadcast_number})
                        sleep(5)
                ref_RTDB.child(f"Broadcast/{ref.id}").update({"is_task_finished":True})
                return {"message":"Sent Succesfully"}
        else:
                decode_msg=Json_data["message"].encode('utf-8').decode("latin-1")
                broadcast_number=0
                broadcast_name=",".join(Json_data["receiver"])
                ref=dataBase.collection("products").document()
                ref.set({
                    "broadcast_name": broadcast_name,
                    "created_at": datetime.now(tz=ZoneInfo('Asia/Kolkata'))
                })
                ref_RTDB.child(f"Broadcast/{ref.id}").update({
                    "total_receiver":len(Json_data["receiver"]),
                    "is_task_finished":False,
                    "send_count":broadcast_number
                })
                for receiver in Json_data["receiver"]:
                    send_message(Json_data,receiver,ref,decode_msg,broadcast_number)
                ref_RTDB.child(f"Broadcast/{ref.id}").update({"is_task_finished":True})
                return {"message":"Sent Succesfully"}
    else:
        return {"error":"Missing parameters(receiver,message,attachments)"}

@app.get("/healthz")
async def root():
    return "ok"

@app.post("/")
async def get_body(request: Request):
    data=await request.json()
    return broadcast(data)