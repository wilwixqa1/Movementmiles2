from fastapi import FastAPI, HTTPException, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional
import httpx
import os
import asyncpg
import io
import csv
from datetime import datetime, timezone

app = FastAPI(title="Movement & Miles")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "mmadmin2026")

# --- Database ---

db_pool = None

@app.on_event("startup")
async def startup():
    global db_pool
    if DATABASE_URL:
        try:
            db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS leads (
                        id SERIAL PRIMARY KEY,
                        first_name TEXT,
                        email TEXT,
                        experience_level TEXT,
                        goals TEXT,
                        referral_source TEXT,
                        recommended_plan TEXT,
                        extra TEXT,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS page_views (
                        id SERIAL PRIMARY KEY,
                        page TEXT NOT NULL,
                        path TEXT,
                        referrer TEXT,
                        user_agent TEXT,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS chat_sessions (
                        id SERIAL PRIMARY KEY,
                        session_type TEXT NOT NULL,
                        message_count INTEGER DEFAULT 1,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
            print("Database connected and tables ready")
        except Exception as e:
            print(f"Database connection failed: {e}")
            db_pool = None
    else:
        print("No DATABASE_URL set — running without database")

@app.on_event("shutdown")
async def shutdown():
    global db_pool
    if db_pool:
        await db_pool.close()


# --- Helpers ---

def check_admin(password: str):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid admin password")


async def call_anthropic(system_prompt: str, messages: list) -> str:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 800,
                "system": system_prompt,
                "messages": messages,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["content"][0]["text"]


# --- System Prompts ---

NELLY_SYSTEM_PROMPT = """You are Nelly, the AI coaching assistant for Movement & Miles (M&M), a holistic running and fitness app created by coach Meg.

PERSONALITY: Warm, encouraging, conversational. You talk like a friend who happens to be a running coach. Keep responses SHORT (2-4 sentences max). Never dump walls of text.

CRITICAL CONVERSATION RULE: Ask ONE question at a time. Never list multiple questions. Have a natural back-and-forth conversation. Guide them step by step.

LINK FORMAT: When recommending a program or answering about a page, include a clickable link using this format:
[[page:PageName]]
Available pages: [[page:Training Programs]], [[page:Race Plans]], [[page:Store]]
Examples:
"You can find it right here: [[page:Training Programs]]"
"Check out all the race options: [[page:Race Plans]]"
"Grab the M&M Bands Kit in the [[page:Store]]"
Always include the relevant page link when recommending a specific program so users can go straight there.

BUTTON FORMAT: When you want to give the user options to choose from, end your message with options in this exact format on a new line:
[Option A | Option B | Option C]

Examples:
"Welcome! Are you looking to start a new program, or do you have a question about the app?"
[I need a training plan | I have a question | I just finished a program]

"Got it! What type of training are you interested in?"
[Running + strength | Strength only | Train for a race]

ONLY use options when there are clear choices. For open-ended questions (like "when is your race?"), just ask normally without options.

CRITICAL: You may ONLY recommend programs that exist in the app. NEVER invent program names.

COMPLETE PROGRAM LIST:

RUNNING + STRENGTH MONTHLY PLANS:
Beginner: Walk to Run Part 1, Walk to Run Part 2, Miles + Bodyweight Strength, Building Endurance & Strength, Beginners: Total Package
Intermediate: Strides + Calisthenics, Outdoor Miles + Weights, Balanced Strides & Strength, Endurance & Strength
Advanced: Run + Lift, Endurance Speed & Strength, Peak Endurance & Power, 7 Weeks to 10 Miles

STRENGTH-ONLY PLANS:
Beginner: Bodyweight & Bands (4wk), Strength Starts Here (2wk), Pure Strength (6wk)
Intermediate: Stronger Strides (6wk), Total Body Power (6wk), Well Built
Advanced: Total Power & Strength (6wk), Cross-Training Power

MOBILITY & PREHAB:
All Levels: Prehab: Knee/ITBS + Mobility (4wk), Mobility Master (4wk), Trail/Road Running Prehab Essentials (6wk), Ankle Foot and Calf Strength (10 modules), ITBS/Knee Pain Workout Collection (10 modules), The Ultimate Mobility Plan (4wk)

RACE PLANS:
Beginner: 8-Week Beginner 5K Treadmill & Outdoor, Beginner 5K Plan (Outdoor), Beginner 10K Plan (Tread & Outdoor), Beginner 10K Plan (Outdoor), Beginner Half Marathon Plan (20wk), Beginner Marathon Plan (20wk)
Intermediate: Intermediate 5K Plan (Tread & Outdoor), Intermediate 5K Plan (Outdoor), Intermediate 10K Plan (Tread & Outdoor), Intermediate 10K Plan (Outdoor), Intermediate Half Marathon Plan (10wk), Intermediate 16-Week Marathon Plan
Advanced: Advanced Half Marathon (12wk), Advanced Marathon Plan, 50K Race Plan (16wk)

DETRAINING PLANS:
Beginner: Detrain Protocol
Intermediate: Recover, Restore & Reset
Advanced: The Adaptation Block

NUTRITION PLANS (mention only when asked or as a last add-on):
Endurance Nutrition, Strength Nutrition, Weight Loss Nutrition

PROGRESSIONS (follow EXACTLY):
BEGINNER RUNNING: Walk to Run Part 1 > Walk to Run Part 2 > Miles + Bodyweight Strength > Building Endurance & Strength > Beginners: Total Package
INTERMEDIATE RUNNING: Strides + Calisthenics > Outdoor Miles + Weights > Balanced Strides & Strength > Endurance & Strength
ADVANCED RUNNING: Run + Lift > Endurance Speed & Strength > Peak Endurance & Power > 7 Weeks to 10 Miles
BEGINNER STRENGTH: Bodyweight & Bands > Strength Starts Here > Pure Strength
INTERMEDIATE STRENGTH: Stronger Strides > Total Body Power > Well Built
ADVANCED STRENGTH: Total Power & Strength > Cross-Training Power
RACE ORDER: 5K > 10K > Half Marathon > Marathon > 50K (never skip)

DETRAINING RULES:
- After running/race program AND person has NOT taken 3+ weeks off: recommend detraining first
- After running/race program AND person HAS taken 3+ weeks off: skip detraining
- After strength-only program: NO detraining needed
- Beginner: Detrain Protocol | Intermediate: Recover, Restore & Reset | Advanced: The Adaptation Block
- After marathon: offer both intermediate and advanced detrain options

EQUIPMENT: Ask about weights (kettlebells or dumbbells; add barbell for advanced). Ask about treadmill preference.
PROGRAMS WITH WEIGHTS: Walk to Run Part 2, Building Endurance & Strength, Beginners: Total Package, Strength Starts Here, Pure Strength, all intermediate+ running, all race programs
BODYWEIGHT-ONLY: Walk to Run Part 1, Miles + Bodyweight Strength, Bodyweight & Bands, Strides + Calisthenics
TREADMILL: Beginners: Total Package, Beginner 5K Tread/Outdoor, Beginner 10K Tread/Outdoor, Int 5K Tread/Outdoor, Int 10K Tread/Outdoor
No weights = only bodyweight programs. No treadmill = no treadmill programs.

PAIN: Mild = prehab + lower-mileage running. Moderate/severe = prehab alone, no running.

CONVERSATION FLOW for "I'm new" or plan recommendations:
When someone says they want a plan, FIRST set expectations with something like: "I'd love to help! I'll ask you a few quick questions to find the perfect plan for you." Then flow into the first question naturally in the same message.
Ask these ONE AT A TIME with buttons where appropriate:
1. Running+strength, strength only, or train for a race?
2. What level? (Beginner/Intermediate/Advanced/Not sure)
3. Can you run 3 miles without stopping? (always say 3)
4. Any pain? (with severity)
5. Access to weights?
6. Treadmill preference?
7. If race: when is it and what distance?
Max 7 questions, then give 3 OPTIONS with brief reasoning.

FLOW for "I just finished a program":
When someone says they just finished a program, acknowledge them warmly first: "Nice work finishing that program! Let me ask a couple quick things so I can point you to the right next step."
1. Which program?
2. Time off since finishing?
3. Goal now?
4. Any pain?
Apply detraining rules. Give 3 options.

RACE RULES: Always ask WHEN. If too soon for plan duration, suggest skipping weeks or later race. For 50K: ask marathon experience and longest run.

FAQs:
CANCEL: Apple/Google > subscription settings. Website > app profile > Info > Manage Subscription
PRICING: Monthly $19.99, Annual $179.99
INCLUDED: Everything - all programs, plans, nutrition
GARMIN/ANNUAL SWITCH: email support@movementandmiles.com
PAYMENT: https://movementandmiles.ymove.app/account
MISSED WORKOUTS: 1-2 = continue. 3-5 = resume easier. Week+ = repeat previous week.

FINAL RECOMMENDATIONS: Always present exactly 3 options, each with a one-sentence explanation of why it fits. Use the button format:
[Option 1 name | Option 2 name | Option 3 name]

Remember: be conversational, one question at a time, short responses, use buttons for choices."""


ONBOARD_SYSTEM_PROMPT = """You are Nelly, the onboarding assistant for Movement & Miles (M&M), a holistic running and fitness app created by coach Meg.

PERSONALITY: Warm, encouraging, brief. Keep every reply to 1-3 sentences. You are guiding them through sign-up.

CONVERSATION FLOW — collect these ONE AT A TIME:
1. Greet warmly. Ask their first name.
2. Ask for their email address.
3. Ask about their fitness goals.
[Run more | Get stronger | Train for a race | Injury recovery | General fitness]
4. Ask about experience level.
[Beginner | Intermediate | Advanced]
5. Ask about equipment access.
[Bodyweight only | Dumbbells/kettlebells | Full gym setup]
6. Ask how they heard about M&M.
[Instagram | TikTok | Friend/referral | Google search | Other]

After collecting all info, recommend ONE plan based on their answers. Use the same program names from the catalog. Then emit the lead tag on its own line at the very end of your message:

[[LEAD:{"first_name":"NAME","email":"EMAIL","experience_level":"LEVEL","goals":"GOALS","referral_source":"SOURCE","recommended_plan":"PLAN"}]]

BUTTON FORMAT: End messages with options when applicable:
[Option A | Option B | Option C]

RULES:
- ONE question per message
- Never skip a step
- Keep it conversational and warm
- The lead tag must be valid JSON inside [[LEAD:...]]
- After emitting lead tag, say something encouraging about getting started"""


# --- Models ---

class ChatRequest(BaseModel):
    message: str
    history: list = []

class ChatResponse(BaseModel):
    reply: str

class LeadData(BaseModel):
    first_name: str = ""
    email: str = ""
    experience_level: str = ""
    goals: str = ""
    referral_source: str = ""
    recommended_plan: str = ""
    extra: str = ""

class PageViewData(BaseModel):
    page: str
    path: str = ""
    referrer: str = ""


# --- API Routes (defined BEFORE static mount) ---

@app.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="API key not configured")
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    messages = []
    for msg in req.history[-20:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": req.message})

    # Track chat session
    if db_pool and len(messages) == 1:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO chat_sessions (session_type) VALUES ($1)", "widget"
                )
        except Exception:
            pass

    try:
        reply_text = await call_anthropic(NELLY_SYSTEM_PROMPT, messages)
        return ChatResponse(reply=reply_text)
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Anthropic API error: {e.response.status_code}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/onboard-chat", response_model=ChatResponse)
async def onboard_chat(req: ChatRequest):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="API key not configured")
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    messages = []
    for msg in req.history[-20:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": req.message})

    # Track chat session
    if db_pool and len(messages) == 1:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO chat_sessions (session_type) VALUES ($1)", "onboard"
                )
        except Exception:
            pass

    try:
        reply_text = await call_anthropic(ONBOARD_SYSTEM_PROMPT, messages)
        return ChatResponse(reply=reply_text)
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Anthropic API error: {e.response.status_code}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/lead")
async def save_lead(lead: LeadData):
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO leads (first_name, email, experience_level, goals, referral_source, recommended_plan, extra)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)""",
                    lead.first_name, lead.email, lead.experience_level,
                    lead.goals, lead.referral_source, lead.recommended_plan, lead.extra
                )
            return {"status": "saved", "storage": "postgres"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    else:
        # Fallback to JSON file
        import json
        leads = []
        try:
            with open("leads.json", "r") as f:
                leads = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            leads = []
        lead_dict = lead.dict()
        lead_dict["timestamp"] = datetime.now(timezone.utc).isoformat()
        leads.append(lead_dict)
        with open("leads.json", "w") as f:
            json.dump(leads, f, indent=2)
        return {"status": "saved", "storage": "json"}


@app.post("/api/page-view")
async def track_page_view(pv: PageViewData, request: Request):
    if not db_pool:
        return {"status": "skipped", "reason": "no database"}
    try:
        ua = request.headers.get("user-agent", "")
        async with db_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO page_views (page, path, referrer, user_agent)
                   VALUES ($1, $2, $3, $4)""",
                pv.page, pv.path, pv.referrer, ua
            )
        return {"status": "tracked"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.get("/api/health")
async def health():
    db_status = "connected" if db_pool else "not connected"
    return {"status": "ok", "service": "Movement & Miles", "version": "6.0", "database": db_status}


# --- Admin API ---

@app.post("/api/admin/login")
async def admin_login(request: Request):
    body = await request.json()
    pw = body.get("password", "")
    if pw != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    return {"status": "ok"}


@app.get("/api/admin/stats")
async def admin_stats(x_admin_password: Optional[str] = Header(None)):
    check_admin(x_admin_password or "")
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        # Leads
        total_leads = await conn.fetchval("SELECT COUNT(*) FROM leads")
        recent_leads = await conn.fetch(
            """SELECT id, first_name, email, experience_level, goals,
                      recommended_plan, referral_source, created_at
               FROM leads ORDER BY created_at DESC LIMIT 50"""
        )

        # Page views — today and total
        total_views = await conn.fetchval("SELECT COUNT(*) FROM page_views")
        today_views = await conn.fetchval(
            "SELECT COUNT(*) FROM page_views WHERE created_at::date = CURRENT_DATE"
        )

        # Page views by page (last 7 days)
        views_by_page = await conn.fetch(
            """SELECT page, COUNT(*) as views
               FROM page_views
               WHERE created_at > NOW() - INTERVAL '7 days'
               GROUP BY page ORDER BY views DESC"""
        )

        # Page views by day (last 7 days)
        views_by_day = await conn.fetch(
            """SELECT created_at::date as day, COUNT(*) as views
               FROM page_views
               WHERE created_at > NOW() - INTERVAL '7 days'
               GROUP BY day ORDER BY day"""
        )

        # Chat sessions
        total_chats = await conn.fetchval("SELECT COUNT(*) FROM chat_sessions")
        chats_by_type = await conn.fetch(
            """SELECT session_type, COUNT(*) as count
               FROM chat_sessions GROUP BY session_type"""
        )

    return {
        "leads": {
            "total": total_leads,
            "recent": [dict(r) for r in recent_leads],
        },
        "page_views": {
            "total": total_views,
            "today": today_views,
            "by_page": [dict(r) for r in views_by_page],
            "by_day": [{"day": str(r["day"]), "views": r["views"]} for r in views_by_day],
        },
        "chats": {
            "total": total_chats,
            "by_type": [dict(r) for r in chats_by_type],
        },
    }


@app.get("/api/admin/leads-csv")
async def admin_leads_csv(x_admin_password: Optional[str] = Header(None)):
    check_admin(x_admin_password or "")
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT first_name, email, experience_level, goals,
                      recommended_plan, referral_source, created_at
               FROM leads ORDER BY created_at DESC"""
        )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["First Name", "Email", "Level", "Goals", "Recommended Plan", "Referral", "Date"])
    for r in rows:
        writer.writerow([
            r["first_name"], r["email"], r["experience_level"],
            r["goals"], r["recommended_plan"], r["referral_source"],
            str(r["created_at"])
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=mm-leads.csv"},
    )


# --- Static Site ---

@app.get("/")
async def root():
    return FileResponse("static/index.html")

@app.get("/mm-admin")
async def admin_page():
    return FileResponse("static/admin.html")

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/{path:path}")
async def catch_all(path: str):
    return FileResponse("static/index.html")
