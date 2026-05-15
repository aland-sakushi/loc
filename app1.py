from flask import Flask, request, jsonify
from telethon import TelegramClient, types
import asyncio
import threading
import os
import base64
import uuid
import time
from queue import Queue
from dataclasses import dataclass
from typing import Optional, Dict
from datetime import datetime
import requests
from io import BytesIO

# ==========================
# CONFIG
# ==========================
api_id = 36212924
api_hash = "2a93625aa144c212abcea7781b5a1342"
bot_username = "Nexora_AutoAcc_Bot"

DOWNLOAD_FOLDER = "static"
REQUEST_TIMEOUT = 120
UPLOAD_FOLDER = "uploads"

# ==========================
# SETUP FOLDER
# ==========================
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# ==========================
# GLOBAL EVENT LOOP
# ==========================
loop = asyncio.new_event_loop()

def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_loop, args=(loop,), daemon=True).start()

# ==========================
# TELEGRAM CLIENT
# ==========================
client = TelegramClient("main_session", api_id, api_hash)

async def init_client():
    await client.start()
    print("✅ Telegram client connected")

asyncio.run_coroutine_threadsafe(init_client(), loop)

# ==========================
# QUEUE SYSTEM
# ==========================
class SimpleQueue:
    def __init__(self):
        self.queue = Queue()
        self.current_request = None
        self.results = {}
        self.lock = threading.Lock()
        
    def add_request(self, request_id, request_type, **kwargs):
        self.queue.put({
            "request_id": request_id,
            "request_type": request_type,
            "timestamp": time.time(),
            **kwargs
        })
        position = self.queue.qsize()
        print(f"📥 [QUEUE] Request {request_id} added. Type: {request_type}. Position: {position}")
        return position
    
    def get_next(self):
        try:
            return self.queue.get(timeout=1)
        except:
            return None
    
    def complete_request(self, request_id, result):
        with self.lock:
            self.results[request_id] = result
        print(f"✅ [QUEUE] Request {request_id} completed")
    
    def get_result(self, request_id, timeout=150):
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self.lock:
                if request_id in self.results:
                    return self.results.pop(request_id)
            time.sleep(0.5)
        return {"error": "Timeout waiting for result"}

request_queue = SimpleQueue()

async def process_queue():
    print("🔄 Queue processor started")
    
    while True:
        try:
            req = request_queue.get_next()
            
            if req is None:
                await asyncio.sleep(0.5)
                continue
            
            print(f"\n🔄 [PROCESS] Processing request {req['request_id']}")
            print(f"   Type: {req['request_type']}")
            
            if req['request_type'] == 'cek_nomor':
                result = await execute_conversation_cek_nomor(
                    req['first_message'],
                    req['phone'],
                    req['request_id']
                )
            elif req['request_type'] == 'fr4':
                result = await execute_conversation_fr4(
                    req['image_path'],
                    req['request_id']
                )
            else:
                result = {"error": f"Unknown request type: {req['request_type']}"}
            
            request_queue.complete_request(req['request_id'], result)
            print(f"✅ [PROCESS] Request {req['request_id']} finished\n")
            
        except Exception as e:
            print(f"❌ [PROCESS] Error: {e}")
            if 'req' in locals() and req:
                request_queue.complete_request(req['request_id'], {"error": str(e)})
            await asyncio.sleep(1)

async def execute_conversation_cek_nomor(first_message: str, phone_number: str, request_id: str):
    try:
        entity = await client.get_entity(bot_username)
        
        async with client.conversation(entity, timeout=REQUEST_TIMEOUT) as conv:
            print(f"📤 [{request_id}] STEP 1: {first_message}")
            await conv.send_message(first_message)
            await asyncio.sleep(2)
            
            print(f"📤 [{request_id}] STEP 2: {phone_number}")
            await conv.send_message(phone_number)
            
            final_text = None
            image_path = None
            all_responses = []
            
            for attempt in range(15):
                try:
                    msg = await conv.get_response(timeout=10)
                    text = msg.text if msg.text else ""
                    
                    print(f"📩 [{request_id}] Response {attempt+1}: {text[:100] if text else '[Empty]'}")
                    all_responses.append(text)
                    
                    if any(keyword in text.lower() for keyword in ["⌛", "processing", "loading", "mengirim", "⏳"]):
                        continue
                    
                    if msg.photo:
                        file_path = await msg.download_media(file=DOWNLOAD_FOLDER)
                        image_path = file_path
                        print(f"🖼️ [{request_id}] Image saved: {file_path}")
                    
                    if text and len(text.strip()) > 0:
                        final_text = text
                        break
                        
                    if image_path:
                        break
                        
                except asyncio.TimeoutError:
                    continue
            
            if not final_text and not image_path and all_responses:
                for resp in reversed(all_responses):
                    if resp and not any(k in resp.lower() for k in ["⌛", "processing", "loading", "mengirim"]):
                        final_text = resp
                        break
            
            return {
                "text": final_text or "Tidak ada response dari bot",
                "image": image_path,
                "first_message": first_message,
                "phone_number": phone_number,
                "request_id": request_id
            }
            
    except Exception as e:
        return {"error": str(e)}

async def execute_conversation_fr4(image_path: str, request_id: str):
    """FR4 - mengumpulkan semua response LENGKAP tanpa terpotong"""
    try:
        entity = await client.get_entity(bot_username)
        
        async with client.conversation(entity, timeout=REQUEST_TIMEOUT) as conv:
            # Kirim perintah FR4
            print(f"📤 [{request_id}] Sending '👁️ FR 4'")
            await conv.send_message("👁️ FR 4")
            await asyncio.sleep(3)
            
            # Kirim foto
            print(f"📤 [{request_id}] Sending photo")
            await client.send_file(
                entity,
                image_path,
                caption="",
                force_document=False,
                supports_streaming=True
            )
            print(f"📤 [{request_id}] Photo sent")
            
            # Kumpulkan semua response
            all_messages = []
            last_message_time = time.time()
            idle_count = 0
            
            while idle_count < 5:  # Tunggu hingga 5 detik tanpa pesan baru
                try:
                    # Ambil response dengan timeout 2 detik
                    msg = await conv.get_response(timeout=2)
                    
                    if msg.text:
                        # Simpan teks LENGKAP (tidak dipotong)
                        full_text = msg.text
                        print(f"📩 [{request_id}] Got message ({len(full_text)} chars)")
                        # Tampilkan preview saja untuk log
                        preview = full_text[:80] + "..." if len(full_text) > 80 else full_text
                        print(f"   Preview: {preview}")
                        all_messages.append(full_text)
                        last_message_time = time.time()
                        idle_count = 0
                    elif msg.photo:
                        print(f"📸 [{request_id}] Got photo from bot")
                        photo_path = await msg.download_media(file=DOWNLOAD_FOLDER)
                        all_messages.append(f"[PHOTO: {photo_path}]")
                        last_message_time = time.time()
                        idle_count = 0
                    else:
                        # Pesan lain (sticker, dll)
                        print(f"📩 [{request_id}] Got other message type")
                        last_message_time = time.time()
                        idle_count = 0
                        
                except asyncio.TimeoutError:
                    idle_count += 1
                    if idle_count <= 3:
                        print(f"   ⏳ Waiting... ({idle_count}/5)")
            
            print(f"\n📊 [{request_id}] Collected {len(all_messages)} messages total")
            
            # Filter pesan loading/waiting
            filtered_messages = []
            for msg in all_messages:
                if msg.startswith("[PHOTO:"):
                    filtered_messages.append(msg)
                elif not any(keyword in msg.lower() for keyword in 
                           ["memproses", "tunggu", "⌛", "⏳", "processing", "loading", "mengirim", "kirim"]):
                    filtered_messages.append(msg)
            
            # Gabungkan semua teks dengan separator yang jelas
            if filtered_messages:
                # Gabungkan dengan newline ganda untuk memisahkan setiap pesan
                final_text = "\n\n━━━━━━━━━━━━━━━━━━━━\n\n".join(filtered_messages)
                print(f"✅ [{request_id}] Combined {len(filtered_messages)} messages into final text")
                print(f"   Total length: {len(final_text)} characters")
            else:
                final_text = "Tidak ada response dari bot"
            
            # Cek apakah ada photo response
            response_images = []
            for msg in all_messages:
                if msg.startswith("[PHOTO:") and "static" in msg:
                    img_path = msg.replace("[PHOTO: ", "").replace("]", "")
                    if os.path.exists(img_path):
                        response_images.append(img_path)
            
            return {
                "text": final_text,
                "request_id": request_id,
                "image_sent": image_path,
                "total_messages": len(all_messages),
                "filtered_messages": len(filtered_messages),
                "response_images": response_images,
                "raw_messages": all_messages  # Untuk debugging jika perlu
            }
            
    except Exception as e:
        print(f"❌ [{request_id}] Error: {e}")
        return {"error": str(e)}

# Start queue processor
asyncio.run_coroutine_threadsafe(process_queue(), loop)

# ==========================
# FLASK APP
# ==========================
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

@app.route("/api/ceknomor", methods=["GET", "POST"])
def cek_nomor():
    if request.method == "GET":
        operator = request.args.get("operator", "1")
        phone = request.args.get("phone")
    else:
        data = request.get_json()
        operator = data.get("operator", "1")
        phone = data.get("phone")
    
    if not phone:
        return jsonify({"status": "error", "message": "Parameter 'phone' wajib diisi"}), 400
    
    if operator not in ["1", "3"]:
        return jsonify({"status": "error", "message": "Operator tidak valid. Gunakan operator=1 atau operator=3"}), 400
    
    first_message = f"📍 CP ALL Operator {operator}"
    request_id = str(uuid.uuid4())[:8]
    position = request_queue.add_request(request_id, "cek_nomor", first_message=first_message, phone=phone)
    result = request_queue.get_result(request_id, timeout=REQUEST_TIMEOUT + 30)
    
    if "error" in result:
        return jsonify({"status": "error", "request_id": request_id, "message": result["error"]}), 500
    
    response_data = {
        "status": "success",
        "request_id": request_id,
        "queue_position": position,
        "operator": operator,
        "first_message": result.get("first_message"),
        "phone_number": result.get("phone_number"),
        "text": result.get("text")
    }
    
    if result.get("image") and os.path.exists(result["image"]):
        try:
            with open(result["image"], "rb") as img_file:
                img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
                response_data["image_base64"] = img_base64
        except Exception as e:
            response_data["image_error"] = str(e)
    
    return jsonify(response_data)

@app.route("/api/fr4", methods=["POST"])
def fr4_analyze():
    try:
        if 'photo' not in request.files:
            return jsonify({"status": "error", "message": "Parameter 'photo' wajib diisi"}), 400
        
        photo_file = request.files['photo']
        if photo_file.filename == '':
            return jsonify({"status": "error", "message": "Tidak ada file yang dipilih"}), 400
        
        # Simpan file
        filename = f"{uuid.uuid4()}.jpg"
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        photo_file.save(filepath)
        print(f"📸 Photo saved: {filepath}")
        
        request_id = str(uuid.uuid4())[:8]
        position = request_queue.add_request(request_id, "fr4", image_path=filepath)
        result = request_queue.get_result(request_id, timeout=REQUEST_TIMEOUT + 30)
        
        # Hapus file temporary
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except:
            pass
        
        if "error" in result:
            return jsonify({"status": "error", "request_id": request_id, "message": result["error"]}), 500
        
        response_data = {
            "status": "success",
            "request_id": request_id,
            "queue_position": position,
            "text": result.get("text"),
            "total_messages": result.get("total_messages", 0),
            "filtered_messages": result.get("filtered_messages", 0)
        }
        
        # Kirim photo response jika ada
        if result.get("response_images") and len(result["response_images"]) > 0:
            img_path = result["response_images"][0]
            if os.path.exists(img_path):
                try:
                    with open(img_path, "rb") as img_file:
                        img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
                        response_data["response_image_base64"] = img_base64
                except Exception as e:
                    response_data["image_error"] = str(e)
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/fr4/url", methods=["POST"])
def fr4_analyze_url():
    try:
        data = request.get_json()
        if not data or 'image_url' not in data:
            return jsonify({"status": "error", "message": "Parameter 'image_url' wajib diisi"}), 400
        
        image_url = data['image_url']
        
        # Download gambar
        response = requests.get(image_url, timeout=30)
        response.raise_for_status()
        
        filename = f"{uuid.uuid4()}.jpg"
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        print(f"📸 Photo downloaded: {filepath}")
        
        request_id = str(uuid.uuid4())[:8]
        position = request_queue.add_request(request_id, "fr4", image_path=filepath)
        result = request_queue.get_result(request_id, timeout=REQUEST_TIMEOUT + 30)
        
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except:
            pass
        
        if "error" in result:
            return jsonify({"status": "error", "request_id": request_id, "message": result["error"]}), 500
        
        response_data = {
            "status": "success",
            "request_id": request_id,
            "queue_position": position,
            "text": result.get("text"),
            "total_messages": result.get("total_messages", 0),
            "filtered_messages": result.get("filtered_messages", 0)
        }
        
        if result.get("response_images") and len(result["response_images"]) > 0:
            img_path = result["response_images"][0]
            if os.path.exists(img_path):
                try:
                    with open(img_path, "rb") as img_file:
                        img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
                        response_data["response_image_base64"] = img_base64
                except Exception as e:
                    response_data["image_error"] = str(e)
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/queue/status")
def queue_status():
    return jsonify({
        "queue_length": request_queue.queue.qsize(),
        "status": "active"
    })

@app.route("/api/queue/clear", methods=["POST"])
def clear_queue():
    while not request_queue.queue.empty():
        try:
            request_queue.queue.get_nowait()
        except:
            break
    return jsonify({"status": "success", "message": "Queue cleared"})

@app.route("/")
def home():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Telegram Bridge API</title>
        <style>
            body { font-family: monospace; margin: 20px; }
            .endpoint { background: #f0f0f0; padding: 10px; margin: 10px 0; border-radius: 5px; }
            code { background: #e0e0e0; padding: 2px 5px; border-radius: 3px; }
            .success { color: green; }
        </style>
    </head>
    <body>
        <h1>🚀 Telegram Bridge API</h1>
        <h3>Status: <span class="success">✅ Running</span></h3>
        
        <div class="endpoint">
            <b>1. Cek Nomor</b><br>
            <code>GET /api/ceknomor?phone=08123456789&operator=3</code>
        </div>
        
        <div class="endpoint">
            <b>2. FR4 - Upload File</b><br>
            <code>curl -X POST http://localhost:5000/api/fr4 -F "photo=@image.jpg"</code>
        </div>
        
        <div class="endpoint">
            <b>3. FR4 - From URL</b><br>
            <code>curl -X POST http://localhost:5000/api/fr4/url -H "Content-Type: application/json" -d '{"image_url": "https://example.com/photo.jpg"}'</code>
        </div>
        
        <div class="endpoint">
            <b>4. Queue Status</b><br>
            <code>GET /api/queue/status</code>
        </div>
    </body>
    </html>
    '''

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 TELEGRAM BRIDGE API STARTED")
    print("=" * 60)
    time.sleep(2)
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)