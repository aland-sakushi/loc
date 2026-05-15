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
REQUEST_TIMEOUT = 120  # Tambah timeout jadi 120 detik
UPLOAD_FOLDER = "uploads"

# ==========================
# SETUP FOLDER
# ==========================
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# ==========================
# GLOBAL EVENT LOOP (SATU UNTUK SEMUA)
# ==========================
loop = asyncio.new_event_loop()

def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_loop, args=(loop,), daemon=True).start()

# ==========================
# TELEGRAM CLIENT (SINGLE SESSION)
# ==========================
client = TelegramClient("main_session", api_id, api_hash)

async def init_client():
    await client.start()
    print("✅ Telegram client connected")

asyncio.run_coroutine_threadsafe(init_client(), loop)

# ==========================
# SIMPLE QUEUE SYSTEM
# ==========================
class SimpleQueue:
    def __init__(self):
        self.queue = Queue()
        self.current_request = None
        self.results = {}
        self.lock = threading.Lock()
        
    def add_request(self, request_id, request_type, **kwargs):
        """Tambah request ke antrian"""
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
        """Ambil request berikutnya"""
        try:
            return self.queue.get(timeout=1)
        except:
            return None
    
    def complete_request(self, request_id, result):
        """Simpan hasil request"""
        with self.lock:
            self.results[request_id] = result
        print(f"✅ [QUEUE] Request {request_id} completed")
    
    def get_result(self, request_id, timeout=150):
        """Tunggu dan ambil hasil request"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self.lock:
                if request_id in self.results:
                    return self.results.pop(request_id)
            time.sleep(0.5)
        return {"error": "Timeout waiting for result"}

# ==========================
# QUEUE PROCESSOR (WORKER)
# ==========================
request_queue = SimpleQueue()

async def process_queue():
    """Worker yang memproses antrian satu per satu"""
    print("🔄 Queue processor started")
    
    while True:
        try:
            # Ambil request dari antrian
            req = request_queue.get_next()
            
            if req is None:
                await asyncio.sleep(0.5)
                continue
            
            print(f"\n🔄 [PROCESS] Processing request {req['request_id']}")
            print(f"   Type: {req['request_type']}")
            
            # Proses request berdasarkan tipe
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
            
            # Simpan hasil
            request_queue.complete_request(req['request_id'], result)
            
            print(f"✅ [PROCESS] Request {req['request_id']} finished\n")
            
        except Exception as e:
            print(f"❌ [PROCESS] Error: {e}")
            if 'req' in locals() and req:
                request_queue.complete_request(req['request_id'], {"error": str(e)})
            await asyncio.sleep(1)

async def execute_conversation_cek_nomor(first_message: str, phone_number: str, request_id: str):
    """Eksekusi 2-step conversation untuk cek nomor"""
    try:
        # Dapatkan entity bot
        entity = await client.get_entity(bot_username)
        
        # Mulai percakapan
        async with client.conversation(entity, timeout=REQUEST_TIMEOUT) as conv:
            # STEP 1: Kirim pesan pertama
            print(f"📤 [{request_id}] STEP 1: {first_message}")
            await conv.send_message(first_message)
            
            # Tunggu sebentar
            await asyncio.sleep(2)
            
            # STEP 2: Kirim nomor telepon
            print(f"📤 [{request_id}] STEP 2: {phone_number}")
            await conv.send_message(phone_number)
            
            # Ambil response dari bot
            final_text = None
            image_path = None
            all_responses = []
            
            for attempt in range(15):
                try:
                    msg = await conv.get_response(timeout=10)
                    text = msg.text if msg.text else ""
                    
                    print(f"📩 [{request_id}] Response {attempt+1}: {text[:100]}")
                    all_responses.append(text)
                    
                    # Skip pesan loading/processing
                    if any(keyword in text.lower() for keyword in ["⌛", "processing", "loading", "mengirim", "⏳"]):
                        print(f"   ⏳ Skip loading message")
                        continue
                    
                    # Download photo jika ada
                    if msg.photo:
                        file_path = await msg.download_media(file=DOWNLOAD_FOLDER)
                        image_path = file_path
                        print(f"🖼️ [{request_id}] Image saved: {file_path}")
                    
                    # Simpan teks jika ada dan tidak kosong
                    if text and len(text.strip()) > 0:
                        final_text = text
                        print(f"   ✅ Got final text response")
                        break
                    
                    # Jika sudah dapat gambar, break juga
                    if image_path:
                        print(f"   ✅ Got image response")
                        break
                        
                except asyncio.TimeoutError:
                    print(f"   ⏰ Timeout waiting for response {attempt+1}")
                    continue
            
            # Jika tidak dapat response setelah semua percobaan
            if not final_text and not image_path:
                if all_responses:
                    for resp in reversed(all_responses):
                        if resp and not any(k in resp.lower() for k in ["⌛", "processing", "loading", "mengirim"]):
                            final_text = resp
                            break
                else:
                    final_text = "Tidak ada response dari bot"
            
            return {
                "text": final_text,
                "image": image_path,
                "first_message": first_message,
                "phone_number": phone_number,
                "request_id": request_id,
                "all_responses": all_responses
            }
            
    except asyncio.TimeoutError:
        return {"error": f"Timeout: Bot tidak merespon dalam {REQUEST_TIMEOUT} detik"}
    except Exception as e:
        return {"error": str(e)}

async def execute_conversation_fr4(image_path: str, request_id: str):
    """Eksekusi FR4 conversation - mengirim foto sebagai photo biasa ke bot dan mengumpulkan semua response"""
    try:
        # Dapatkan entity bot
        entity = await client.get_entity(bot_username)
        
        # Mulai percakapan
        async with client.conversation(entity, timeout=REQUEST_TIMEOUT) as conv:
            # STEP 1: Kirim pesan "👁️ FR 4"
            print(f"📤 [{request_id}] STEP 1: Sending '👁️ FR 4'")
            await conv.send_message("👁️ FR 4")
            
            # Tunggu sebentar
            await asyncio.sleep(3)
            
            # STEP 2: Kirim foto sebagai PHOTO (bukan file/document)
            print(f"📤 [{request_id}] STEP 2: Sending photo as image")
            
            # Kirim foto sebagai photo biasa
            await client.send_file(
                entity,
                image_path,
                caption="",
                force_document=False,
                supports_streaming=True
            )
            
            print(f"📤 [{request_id}] Photo sent successfully as image")
            
            # Kumpulkan semua response dari bot
            all_text_responses = []
            response_images = []
            last_response_time = time.time()
            no_response_count = 0
            
            # Terus kumpulkan response hingga timeout atau tidak ada response baru
            while True:
                try:
                    # Tunggu response dengan timeout 5 detik
                    msg = await conv.get_response(timeout=5)
                    text = msg.text if msg.text else ""
                    
                    if text:
                        print(f"📩 [{request_id}] Collected: {text[:100]}...")
                        all_text_responses.append(text)
                        last_response_time = time.time()
                        no_response_count = 0
                    elif msg.photo:
                        print(f"📸 [{request_id}] Received photo from bot")
                        photo_path = await msg.download_media(file=DOWNLOAD_FOLDER)
                        response_images.append(photo_path)
                        last_response_time = time.time()
                        no_response_count = 0
                    else:
                        # Ada response tapi bukan text atau photo
                        print(f"📩 [{request_id}] Received non-text/photo message")
                        last_response_time = time.time()
                        no_response_count = 0
                        
                except asyncio.TimeoutError:
                    no_response_count += 1
                    print(f"   ⏰ No response for {no_response_count} seconds")
                    
                    # Jika sudah 3 detik tidak ada response baru, anggap selesai
                    if no_response_count >= 3:
                        print(f"   ✅ No more responses, collecting {len(all_text_responses)} messages")
                        break
                    
                    # Jika sudah lebih dari 10 detik sejak response terakhir, berhenti
                    if time.time() - last_response_time > 10:
                        print(f"   ⏱️ Timeout reached, stopping collection")
                        break
            
            # Gabungkan semua text response
            if all_text_responses:
                # Filter pesan loading
                filtered_responses = []
                for resp in all_text_responses:
                    if not any(keyword in resp.lower() for keyword in ["⌛", "processing", "loading", "mengirim", "⏳", "memproses", "tunggu"]):
                        filtered_responses.append(resp)
                
                # Gabungkan semua response
                final_text = "\n\n".join(filtered_responses)
                print(f"📝 [{request_id}] Combined {len(filtered_responses)} responses into final text")
            else:
                final_text = "Tidak ada response dari bot"
            
            result = {
                "text": final_text,
                "request_id": request_id,
                "all_responses": all_text_responses,
                "image_sent": image_path,
                "total_messages": len(all_text_responses)
            }
            
            # Tambahkan foto response jika ada
            if response_images:
                result["response_images"] = response_images
            
            return result
            
    except asyncio.TimeoutError:
        return {"error": f"Timeout: Bot tidak merespon dalam {REQUEST_TIMEOUT} detik"}
    except Exception as e:
        return {"error": str(e)}

# Start queue processor
asyncio.run_coroutine_threadsafe(process_queue(), loop)

# ==========================
# FLASK APP
# ==========================
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # Max 50MB untuk upload file

@app.route("/api/ceknomor", methods=["GET", "POST"])
def cek_nomor():
    """Cek nomor telepon dengan queue system - Support Operator 1 dan Operator 3"""
    
    # Ambil parameter
    if request.method == "GET":
        operator = request.args.get("operator", "1")
        phone = request.args.get("phone")
    else:
        data = request.get_json()
        operator = data.get("operator", "1")
        phone = data.get("phone")
    
    if not phone:
        return jsonify({
            "status": "error",
            "message": "Parameter 'phone' wajib diisi"
        }), 400
    
    # Validasi operator
    if operator not in ["1", "3"]:
        return jsonify({
            "status": "error",
            "message": "Operator tidak valid. Gunakan operator=1 atau operator=3"
        }), 400
    
    # Tentukan first_message berdasarkan operator
    if operator == "3":
        first_message = "📍 CP ALL Operator 3"
    else:
        first_message = "📍 CP ALL Operator 1"
    
    # Buat request ID
    request_id = str(uuid.uuid4())[:8]
    
    # Masukkan ke antrian
    position = request_queue.add_request(request_id, "cek_nomor", 
                                        first_message=first_message, 
                                        phone=phone)
    
    # Tunggu hasil
    result = request_queue.get_result(request_id, timeout=REQUEST_TIMEOUT + 30)
    
    if "error" in result:
        return jsonify({
            "status": "error",
            "request_id": request_id,
            "message": result["error"]
        }), 500
    
    # Build response
    response_data = {
        "status": "success",
        "request_id": request_id,
        "queue_position": position,
        "operator": operator,
        "first_message": result.get("first_message"),
        "phone_number": result.get("phone_number"),
        "text": result.get("text")
    }
    
    # Convert image ke base64 jika ada
    if result.get("image") and os.path.exists(result["image"]):
        try:
            with open(result["image"], "rb") as img_file:
                img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
                response_data["image_base64"] = img_base64
                response_data["image_type"] = "image/jpeg"
        except Exception as e:
            response_data["image_error"] = str(e)
    
    return jsonify(response_data)

@app.route("/api/fr4", methods=["POST"])
def fr4_analyze():
    """Endpoint untuk FR4 - mengirim foto ke bot"""
    try:
        # Cek apakah ada file foto yang diupload
        if 'photo' not in request.files:
            return jsonify({
                "status": "error",
                "message": "Parameter 'photo' wajib diisi (file gambar)"
            }), 400
        
        photo_file = request.files['photo']
        
        if photo_file.filename == '':
            return jsonify({
                "status": "error",
                "message": "Tidak ada file yang dipilih"
            }), 400
        
        # Validasi tipe file
        allowed_extensions = {'jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp'}
        file_extension = photo_file.filename.rsplit('.', 1)[1].lower() if '.' in photo_file.filename else ''
        
        if file_extension not in allowed_extensions:
            return jsonify({
                "status": "error",
                "message": f"Tipe file tidak支持. Gunakan: {', '.join(allowed_extensions)}"
            }), 400
        
        # Simpan file sementara
        filename = f"{uuid.uuid4()}.{file_extension}"
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        photo_file.save(filepath)
        
        print(f"📸 [FR4] Photo saved: {filepath}")
        
        # Buat request ID
        request_id = str(uuid.uuid4())[:8]
        
        # Masukkan ke antrian
        position = request_queue.add_request(request_id, "fr4", image_path=filepath)
        
        # Tunggu hasil
        result = request_queue.get_result(request_id, timeout=REQUEST_TIMEOUT + 30)
        
        # Hapus file sementara setelah diproses
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except Exception as e:
            print(f"⚠️ Gagal menghapus file temporary: {e}")
        
        if "error" in result:
            return jsonify({
                "status": "error",
                "request_id": request_id,
                "message": result["error"]
            }), 500
        
        # Build response
        response_data = {
            "status": "success",
            "request_id": request_id,
            "queue_position": position,
            "text": result.get("text"),
            "analyzed": True,
            "total_messages": result.get("total_messages", 0)
        }
        
        # Tambahkan response images jika ada (ambil yang pertama)
        if result.get("response_images") and len(result["response_images"]) > 0:
            first_image = result["response_images"][0]
            if os.path.exists(first_image):
                try:
                    with open(first_image, "rb") as img_file:
                        img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
                        response_data["response_image_base64"] = img_base64
                        response_data["response_image_type"] = "image/jpeg"
                except Exception as e:
                    response_data["response_image_error"] = str(e)
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"❌ [FR4] Error: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/api/fr4/url", methods=["POST"])
def fr4_analyze_url():
    """Endpoint untuk FR4 - mengirim foto dari URL ke bot"""
    try:
        data = request.get_json()
        
        if not data or 'image_url' not in data:
            return jsonify({
                "status": "error",
                "message": "Parameter 'image_url' wajib diisi"
            }), 400
        
        image_url = data['image_url']
        
        # Download gambar dari URL
        try:
            response = requests.get(image_url, timeout=30)
            response.raise_for_status()
            
            # Tentukan ekstensi file dari content-type atau URL
            content_type = response.headers.get('content-type', '')
            if 'jpeg' in content_type or 'jpg' in content_type:
                extension = 'jpg'
            elif 'png' in content_type:
                extension = 'png'
            elif 'gif' in content_type:
                extension = 'gif'
            else:
                extension = 'jpg'
            
            filename = f"{uuid.uuid4()}.{extension}"
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            print(f"📸 [FR4] Photo downloaded from URL: {filepath}")
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": f"Gagal mendownload gambar dari URL: {str(e)}"
            }), 400
        
        # Buat request ID
        request_id = str(uuid.uuid4())[:8]
        
        # Masukkan ke antrian
        position = request_queue.add_request(request_id, "fr4", image_path=filepath)
        
        # Tunggu hasil
        result = request_queue.get_result(request_id, timeout=REQUEST_TIMEOUT + 30)
        
        # Hapus file sementara setelah diproses
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except Exception as e:
            print(f"⚠️ Gagal menghapus file temporary: {e}")
        
        if "error" in result:
            return jsonify({
                "status": "error",
                "request_id": request_id,
                "message": result["error"]
            }), 500
        
        # Build response
        response_data = {
            "status": "success",
            "request_id": request_id,
            "queue_position": position,
            "text": result.get("text"),
            "analyzed": True,
            "total_messages": result.get("total_messages", 0)
        }
        
        # Tambahkan response images jika ada
        if result.get("response_images") and len(result["response_images"]) > 0:
            first_image = result["response_images"][0]
            if os.path.exists(first_image):
                try:
                    with open(first_image, "rb") as img_file:
                        img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
                        response_data["response_image_base64"] = img_base64
                        response_data["response_image_type"] = "image/jpeg"
                except Exception as e:
                    response_data["response_image_error"] = str(e)
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"❌ [FR4] Error: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/api/queue/status")
def queue_status():
    """Cek status antrian"""
    queue_size = request_queue.queue.qsize()
    return jsonify({
        "queue_length": queue_size,
        "status": "active",
        "message": f"{queue_size} request(s) waiting"
    })

@app.route("/api/queue/clear", methods=["POST"])
def clear_queue():
    """Clear antrian (emergency)"""
    while not request_queue.queue.empty():
        try:
            request_queue.queue.get_nowait()
        except:
            break
    
    return jsonify({
        "status": "success",
        "message": "Queue cleared"
    })

@app.route("/")
def home():
    queue_size = request_queue.queue.qsize()
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Telegram Bridge API</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { color: #333; }
            h2 { color: #666; margin-top: 30px; }
            .endpoint { background: #f4f4f4; padding: 10px; margin: 10px 0; border-radius: 5px; }
            code { background: #e1e1e1; padding: 2px 5px; border-radius: 3px; }
            .success { color: green; }
            .feature { border-left: 4px solid #4CAF50; padding-left: 15px; margin: 20px 0; }
        </style>
    </head>
    <body>
        <h1>🚀 TELEGRAM BRIDGE API WITH QUEUE SYSTEM</h1>
        
        <div class="feature">
            <h2>✅ Status:</h2>
            <ul>
                <li>Queue Length: <b>''' + str(queue_size) + '''</b></li>
                <li>Telegram Client: <b class="success">✅ Connected</b></li>
                <li>Support Features: <b>Cek Nomor (OP1 & OP3) + FR4 (Facial Recognition)</b></li>
            </ul>
        </div>
        
        <h2>📌 Feature 1: Cek Nomor</h2>
        <div class="endpoint">
            <b>GET/POST /api/ceknomor?phone=08123456789&operator=1</b>
        </div>
        
        <h2>📌 Feature 2: FR4 - Facial Recognition</h2>
        <div class="endpoint">
            <b>Upload file:</b> POST /api/fr4 -F "photo=@image.jpg"<br>
            <b>From URL:</b> POST /api/fr4/url -d '{"image_url": "https://..."}'
        </div>
        
        <h2>📌 Contoh Response FR4:</h2>
        <pre>
{
    "status": "success",
    "text": "Hasil Pencocokan Wajah... (semua data lengkap)",
    "total_messages": 8,
    "response_image_base64": "..."
}
        </pre>
    </body>
    </html>
    '''

# ==========================
# RUN
# ==========================
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 TELEGRAM BRIDGE WITH QUEUE SYSTEM (FIXED)")
    print("=" * 60)
    print(f"Bot username: {bot_username}")
    print(f"Support: Cek Nomor (OP1/OP3) + FR4")
    print(f"Queue: FIFO | Timeout: {REQUEST_TIMEOUT}s")
    print("=" * 60)
    
    time.sleep(2)
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)