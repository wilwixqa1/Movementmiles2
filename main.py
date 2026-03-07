from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import httpx
import os
import json
import datetime

app = FastAPI(title="Movement & Miles")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

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


# ── ONBOARDING NELLY SYSTEM PROMPT ──
ONBOARD_SYSTEM_PROMPT = """You are Nelly, the AI coaching assistant for Movement & Miles (M&M), guiding a new user through onboarding. You are warm, encouraging, and conversational — like a friend who happens to be a running coach.

YOUR GOAL: Walk the user through a friendly conversation to learn about them and recommend the perfect plan. You need to collect specific information, but make it feel natural — NOT like a form.

CRITICAL RULES:
- Ask ONE question at a time. Never list multiple questions.
- Keep responses SHORT (2-3 sentences max).
- Use the button format for choices: [Option A | Option B | Option C]
- Be warm and personal — use their name once you have it.

ONBOARDING FLOW (follow this order):

STEP 1 - GREETING (your first message, already sent):
"Hey! I'm Nelly, your M&M coaching assistant. I'm going to help you find the perfect training plan — it'll only take a minute! First off, what's your name?"

STEP 2 - After they give their name:
Use their name warmly. Ask for their email.
"Love it, [Name]! So I can set things up for you, what's your email address?"

STEP 3 - After email:
Ask what they're looking for.
"Awesome, thanks [Name]! So tell me — what are you looking to get into?"
[Running + strength | Strength only | Train for a race]

STEP 4 - After goal:
Ask experience level.
"And where would you say you're at fitness-wise right now?"
[Beginner | Intermediate | Advanced | Not sure]

STEP 5 - After level (if they say "Not sure"):
Ask: "No worries! Can you run 3 miles without stopping?"
[Yes, easily | I can but it's hard | Not yet]
Map: "Not yet" = Beginner, "hard" = Beginner, "easily" = Intermediate

STEP 6 - Ask about equipment:
"Do you have access to weights like dumbbells or kettlebells?"
[Yes, I have weights | Bodyweight only]

STEP 7 - Ask referral source:
"Almost done! Just curious — how did you hear about Movement & Miles?"
[Instagram | Facebook | Google search | Friend/word of mouth | Other]

STEP 8 - FINAL RECOMMENDATION:
Based on what you've learned, recommend ONE specific program with a brief explanation of why it's perfect for them. Be enthusiastic!

Then include this EXACT tag at the END of your final message (the user won't see it — the frontend will parse it):
[[LEAD:{"first_name":"...","email":"...","experience_level":"...","goals":"...","referral_source":"...","recommended_plan":"..."}]]

IMPORTANT: The [[LEAD:...]] tag must be valid JSON inside the curly braces. Include ALL fields. Use these exact key names.

After the [[LEAD:...]] tag, do NOT add anything else.

PROGRAM KNOWLEDGE (use to make recommendations):

RUNNING + STRENGTH:
Beginner (no weights): Walk to Run Part 1, Miles + Bodyweight Strength
Beginner (weights): Walk to Run Part 2, Building Endurance & Strength, Beginners: Total Package
Intermediate (no weights): Strides + Calisthenics
Intermediate (weights): Outdoor Miles + Weights, Balanced Strides & Strength, Endurance & Strength
Advanced (weights): Run + Lift, Endurance Speed & Strength, Peak Endurance & Power

STRENGTH ONLY:
Beginner (no weights): Bodyweight & Bands
Beginner (weights): Strength Starts Here, Pure Strength
Intermediate: Stronger Strides, Total Body Power
Advanced: Total Power & Strength

RACE TRAINING:
If they say race, ask distance and recommend the appropriate beginner/intermediate/advanced plan.

MOBILITY/PREHAB:
Recommend as an add-on if they mention pain or injury.

PRICING (if asked): Monthly $19.99, Annual $179.99. First month free!

Remember: You're the first impression of M&M. Be warm, be brief, make them excited to start training!"""


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
    extra: dict = {}


# ── Lead storage (JSON file for now, Postgres in Phase 2) ──
LEADS_FILE = "leads.json"


def save_lead(lead: dict):
    """Append a lead to the JSON file."""
    leads = []
    if os.path.exists(LEADS_FILE):
        try:
            with open(LEADS_FILE, "r") as f:
                leads = json.load(f)
        except (json.JSONDecodeError, IOError):
            leads = []
    lead["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"
    leads.append(lead)
    with open(LEADS_FILE, "w") as f:
        json.dump(leads, f, indent=2)
    return lead


# --- API Routes (defined BEFORE static mount so they take priority) ---

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

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
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
                    "system": NELLY_SYSTEM_PROMPT,
                    "messages": messages,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            reply_text = data["content"][0]["text"]
            return ChatResponse(reply=reply_text)
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=502, detail=f"Anthropic API error: {e.response.status_code}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/onboard-chat", response_model=ChatResponse)
async def onboard_chat(req: ChatRequest):
    """Onboarding Nelly — uses a different system prompt to guide new user signup."""
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="API key not configured")

    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    messages = []
    for msg in req.history[-20:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": req.message})

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
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
                    "system": ONBOARD_SYSTEM_PROMPT,
                    "messages": messages,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            reply_text = data["content"][0]["text"]
            return ChatResponse(reply=reply_text)
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=502, detail=f"Anthropic API error: {e.response.status_code}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/lead")
async def capture_lead(lead: LeadData):
    """Save a lead from the onboarding flow."""
    lead_dict = lead.dict()
    saved = save_lead(lead_dict)
    return {"status": "ok", "lead": saved}


@app.get("/api/leads")
async def get_leads():
    """Get all leads (for admin/debugging — will be protected in Phase 2)."""
    if not os.path.exists(LEADS_FILE):
        return {"leads": [], "count": 0}
    try:
        with open(LEADS_FILE, "r") as f:
            leads = json.load(f)
        return {"leads": leads, "count": len(leads)}
    except (json.JSONDecodeError, IOError):
        return {"leads": [], "count": 0}


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "Movement & Miles", "version": "4.1"}


# --- Static Site ---

@app.get("/")
async def root():
    return FileResponse("static/index.html")


# Mount static directory for any future assets (CSS, JS, images)
app.mount("/static", StaticFiles(directory="static"), name="static")


# Catch-all: serve index.html for all other paths (client-side routing)
@app.get("/{path:path}")
async def catch_all(path: str):
    return FileResponse("static/index.html")
