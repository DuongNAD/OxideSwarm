//! 2048-entry English word list and short pairing code grammar (`JOIN-2a`, design record
//! `docs/design/0052-short-code-pairing.md` D1; shared by `USB-1`, design `0053`).
//!
//! Word list source: Bitcoin BIP-0039 English wordlist copied verbatim from
//! `rust-bitcoin/rust-bip39` v3.0.0 (`CC0-1.0` Public Domain dedication, compatible with
//! `MIT OR Apache-2.0`).

use std::fmt;

use ring::rand::{SecureRandom, SystemRandom};

use rusty_grid_core::error::{GridError, GridResult};

/// Lowest valid pairing nameplate (`1..=999`).
pub const MIN_NAMEPLATE: u16 = 1;
/// Highest valid pairing nameplate (`1..=999`).
pub const MAX_NAMEPLATE: u16 = 999;

/// BIP-0039 English word list (2048 strictly sorted lowercase ASCII words).
pub const WORDS: [&str; 2048] = [
    "abandon", "ability", "able", "about", "above", "absent", "absorb", "abstract", "absurd",
    "abuse", "access", "accident", "account", "accuse", "achieve", "acid", "acoustic", "acquire",
    "across", "act", "action", "actor", "actress", "actual", "adapt", "add", "addict", "address",
    "adjust", "admit", "adult", "advance", "advice", "aerobic", "affair", "afford", "afraid",
    "again", "age", "agent", "agree", "ahead", "aim", "air", "airport", "aisle", "alarm", "album",
    "alcohol", "alert", "alien", "all", "alley", "allow", "almost", "alone", "alpha", "already",
    "also", "alter", "always", "amateur", "amazing", "among", "amount", "amused", "analyst",
    "anchor", "ancient", "anger", "angle", "angry", "animal", "ankle", "announce", "annual",
    "another", "answer", "antenna", "antique", "anxiety", "any", "apart", "apology", "appear",
    "apple", "approve", "april", "arch", "arctic", "area", "arena", "argue", "arm", "armed",
    "armor", "army", "around", "arrange", "arrest", "arrive", "arrow", "art", "artefact", "artist",
    "artwork", "ask", "aspect", "assault", "asset", "assist", "assume", "asthma", "athlete",
    "atom", "attack", "attend", "attitude", "attract", "auction", "audit", "august", "aunt",
    "author", "auto", "autumn", "average", "avocado", "avoid", "awake", "aware", "away", "awesome",
    "awful", "awkward", "axis", "baby", "bachelor", "bacon", "badge", "bag", "balance", "balcony",
    "ball", "bamboo", "banana", "banner", "bar", "barely", "bargain", "barrel", "base", "basic",
    "basket", "battle", "beach", "bean", "beauty", "because", "become", "beef", "before", "begin",
    "behave", "behind", "believe", "below", "belt", "bench", "benefit", "best", "betray", "better",
    "between", "beyond", "bicycle", "bid", "bike", "bind", "biology", "bird", "birth", "bitter",
    "black", "blade", "blame", "blanket", "blast", "bleak", "bless", "blind", "blood", "blossom",
    "blouse", "blue", "blur", "blush", "board", "boat", "body", "boil", "bomb", "bone", "bonus",
    "book", "boost", "border", "boring", "borrow", "boss", "bottom", "bounce", "box", "boy",
    "bracket", "brain", "brand", "brass", "brave", "bread", "breeze", "brick", "bridge", "brief",
    "bright", "bring", "brisk", "broccoli", "broken", "bronze", "broom", "brother", "brown",
    "brush", "bubble", "buddy", "budget", "buffalo", "build", "bulb", "bulk", "bullet", "bundle",
    "bunker", "burden", "burger", "burst", "bus", "business", "busy", "butter", "buyer", "buzz",
    "cabbage", "cabin", "cable", "cactus", "cage", "cake", "call", "calm", "camera", "camp", "can",
    "canal", "cancel", "candy", "cannon", "canoe", "canvas", "canyon", "capable", "capital",
    "captain", "car", "carbon", "card", "cargo", "carpet", "carry", "cart", "case", "cash",
    "casino", "castle", "casual", "cat", "catalog", "catch", "category", "cattle", "caught",
    "cause", "caution", "cave", "ceiling", "celery", "cement", "census", "century", "cereal",
    "certain", "chair", "chalk", "champion", "change", "chaos", "chapter", "charge", "chase",
    "chat", "cheap", "check", "cheese", "chef", "cherry", "chest", "chicken", "chief", "child",
    "chimney", "choice", "choose", "chronic", "chuckle", "chunk", "churn", "cigar", "cinnamon",
    "circle", "citizen", "city", "civil", "claim", "clap", "clarify", "claw", "clay", "clean",
    "clerk", "clever", "click", "client", "cliff", "climb", "clinic", "clip", "clock", "clog",
    "close", "cloth", "cloud", "clown", "club", "clump", "cluster", "clutch", "coach", "coast",
    "coconut", "code", "coffee", "coil", "coin", "collect", "color", "column", "combine", "come",
    "comfort", "comic", "common", "company", "concert", "conduct", "confirm", "congress",
    "connect", "consider", "control", "convince", "cook", "cool", "copper", "copy", "coral",
    "core", "corn", "correct", "cost", "cotton", "couch", "country", "couple", "course", "cousin",
    "cover", "coyote", "crack", "cradle", "craft", "cram", "crane", "crash", "crater", "crawl",
    "crazy", "cream", "credit", "creek", "crew", "cricket", "crime", "crisp", "critic", "crop",
    "cross", "crouch", "crowd", "crucial", "cruel", "cruise", "crumble", "crunch", "crush", "cry",
    "crystal", "cube", "culture", "cup", "cupboard", "curious", "current", "curtain", "curve",
    "cushion", "custom", "cute", "cycle", "dad", "damage", "damp", "dance", "danger", "daring",
    "dash", "daughter", "dawn", "day", "deal", "debate", "debris", "decade", "december", "decide",
    "decline", "decorate", "decrease", "deer", "defense", "define", "defy", "degree", "delay",
    "deliver", "demand", "demise", "denial", "dentist", "deny", "depart", "depend", "deposit",
    "depth", "deputy", "derive", "describe", "desert", "design", "desk", "despair", "destroy",
    "detail", "detect", "develop", "device", "devote", "diagram", "dial", "diamond", "diary",
    "dice", "diesel", "diet", "differ", "digital", "dignity", "dilemma", "dinner", "dinosaur",
    "direct", "dirt", "disagree", "discover", "disease", "dish", "dismiss", "disorder", "display",
    "distance", "divert", "divide", "divorce", "dizzy", "doctor", "document", "dog", "doll",
    "dolphin", "domain", "donate", "donkey", "donor", "door", "dose", "double", "dove", "draft",
    "dragon", "drama", "drastic", "draw", "dream", "dress", "drift", "drill", "drink", "drip",
    "drive", "drop", "drum", "dry", "duck", "dumb", "dune", "during", "dust", "dutch", "duty",
    "dwarf", "dynamic", "eager", "eagle", "early", "earn", "earth", "easily", "east", "easy",
    "echo", "ecology", "economy", "edge", "edit", "educate", "effort", "egg", "eight", "either",
    "elbow", "elder", "electric", "elegant", "element", "elephant", "elevator", "elite", "else",
    "embark", "embody", "embrace", "emerge", "emotion", "employ", "empower", "empty", "enable",
    "enact", "end", "endless", "endorse", "enemy", "energy", "enforce", "engage", "engine",
    "enhance", "enjoy", "enlist", "enough", "enrich", "enroll", "ensure", "enter", "entire",
    "entry", "envelope", "episode", "equal", "equip", "era", "erase", "erode", "erosion", "error",
    "erupt", "escape", "essay", "essence", "estate", "eternal", "ethics", "evidence", "evil",
    "evoke", "evolve", "exact", "example", "excess", "exchange", "excite", "exclude", "excuse",
    "execute", "exercise", "exhaust", "exhibit", "exile", "exist", "exit", "exotic", "expand",
    "expect", "expire", "explain", "expose", "express", "extend", "extra", "eye", "eyebrow",
    "fabric", "face", "faculty", "fade", "faint", "faith", "fall", "false", "fame", "family",
    "famous", "fan", "fancy", "fantasy", "farm", "fashion", "fat", "fatal", "father", "fatigue",
    "fault", "favorite", "feature", "february", "federal", "fee", "feed", "feel", "female",
    "fence", "festival", "fetch", "fever", "few", "fiber", "fiction", "field", "figure", "file",
    "film", "filter", "final", "find", "fine", "finger", "finish", "fire", "firm", "first",
    "fiscal", "fish", "fit", "fitness", "fix", "flag", "flame", "flash", "flat", "flavor", "flee",
    "flight", "flip", "float", "flock", "floor", "flower", "fluid", "flush", "fly", "foam",
    "focus", "fog", "foil", "fold", "follow", "food", "foot", "force", "forest", "forget", "fork",
    "fortune", "forum", "forward", "fossil", "foster", "found", "fox", "fragile", "frame",
    "frequent", "fresh", "friend", "fringe", "frog", "front", "frost", "frown", "frozen", "fruit",
    "fuel", "fun", "funny", "furnace", "fury", "future", "gadget", "gain", "galaxy", "gallery",
    "game", "gap", "garage", "garbage", "garden", "garlic", "garment", "gas", "gasp", "gate",
    "gather", "gauge", "gaze", "general", "genius", "genre", "gentle", "genuine", "gesture",
    "ghost", "giant", "gift", "giggle", "ginger", "giraffe", "girl", "give", "glad", "glance",
    "glare", "glass", "glide", "glimpse", "globe", "gloom", "glory", "glove", "glow", "glue",
    "goat", "goddess", "gold", "good", "goose", "gorilla", "gospel", "gossip", "govern", "gown",
    "grab", "grace", "grain", "grant", "grape", "grass", "gravity", "great", "green", "grid",
    "grief", "grit", "grocery", "group", "grow", "grunt", "guard", "guess", "guide", "guilt",
    "guitar", "gun", "gym", "habit", "hair", "half", "hammer", "hamster", "hand", "happy",
    "harbor", "hard", "harsh", "harvest", "hat", "have", "hawk", "hazard", "head", "health",
    "heart", "heavy", "hedgehog", "height", "hello", "helmet", "help", "hen", "hero", "hidden",
    "high", "hill", "hint", "hip", "hire", "history", "hobby", "hockey", "hold", "hole", "holiday",
    "hollow", "home", "honey", "hood", "hope", "horn", "horror", "horse", "hospital", "host",
    "hotel", "hour", "hover", "hub", "huge", "human", "humble", "humor", "hundred", "hungry",
    "hunt", "hurdle", "hurry", "hurt", "husband", "hybrid", "ice", "icon", "idea", "identify",
    "idle", "ignore", "ill", "illegal", "illness", "image", "imitate", "immense", "immune",
    "impact", "impose", "improve", "impulse", "inch", "include", "income", "increase", "index",
    "indicate", "indoor", "industry", "infant", "inflict", "inform", "inhale", "inherit",
    "initial", "inject", "injury", "inmate", "inner", "innocent", "input", "inquiry", "insane",
    "insect", "inside", "inspire", "install", "intact", "interest", "into", "invest", "invite",
    "involve", "iron", "island", "isolate", "issue", "item", "ivory", "jacket", "jaguar", "jar",
    "jazz", "jealous", "jeans", "jelly", "jewel", "job", "join", "joke", "journey", "joy", "judge",
    "juice", "jump", "jungle", "junior", "junk", "just", "kangaroo", "keen", "keep", "ketchup",
    "key", "kick", "kid", "kidney", "kind", "kingdom", "kiss", "kit", "kitchen", "kite", "kitten",
    "kiwi", "knee", "knife", "knock", "know", "lab", "label", "labor", "ladder", "lady", "lake",
    "lamp", "language", "laptop", "large", "later", "latin", "laugh", "laundry", "lava", "law",
    "lawn", "lawsuit", "layer", "lazy", "leader", "leaf", "learn", "leave", "lecture", "left",
    "leg", "legal", "legend", "leisure", "lemon", "lend", "length", "lens", "leopard", "lesson",
    "letter", "level", "liar", "liberty", "library", "license", "life", "lift", "light", "like",
    "limb", "limit", "link", "lion", "liquid", "list", "little", "live", "lizard", "load", "loan",
    "lobster", "local", "lock", "logic", "lonely", "long", "loop", "lottery", "loud", "lounge",
    "love", "loyal", "lucky", "luggage", "lumber", "lunar", "lunch", "luxury", "lyrics", "machine",
    "mad", "magic", "magnet", "maid", "mail", "main", "major", "make", "mammal", "man", "manage",
    "mandate", "mango", "mansion", "manual", "maple", "marble", "march", "margin", "marine",
    "market", "marriage", "mask", "mass", "master", "match", "material", "math", "matrix",
    "matter", "maximum", "maze", "meadow", "mean", "measure", "meat", "mechanic", "medal", "media",
    "melody", "melt", "member", "memory", "mention", "menu", "mercy", "merge", "merit", "merry",
    "mesh", "message", "metal", "method", "middle", "midnight", "milk", "million", "mimic", "mind",
    "minimum", "minor", "minute", "miracle", "mirror", "misery", "miss", "mistake", "mix", "mixed",
    "mixture", "mobile", "model", "modify", "mom", "moment", "monitor", "monkey", "monster",
    "month", "moon", "moral", "more", "morning", "mosquito", "mother", "motion", "motor",
    "mountain", "mouse", "move", "movie", "much", "muffin", "mule", "multiply", "muscle", "museum",
    "mushroom", "music", "must", "mutual", "myself", "mystery", "myth", "naive", "name", "napkin",
    "narrow", "nasty", "nation", "nature", "near", "neck", "need", "negative", "neglect",
    "neither", "nephew", "nerve", "nest", "net", "network", "neutral", "never", "news", "next",
    "nice", "night", "noble", "noise", "nominee", "noodle", "normal", "north", "nose", "notable",
    "note", "nothing", "notice", "novel", "now", "nuclear", "number", "nurse", "nut", "oak",
    "obey", "object", "oblige", "obscure", "observe", "obtain", "obvious", "occur", "ocean",
    "october", "odor", "off", "offer", "office", "often", "oil", "okay", "old", "olive", "olympic",
    "omit", "once", "one", "onion", "online", "only", "open", "opera", "opinion", "oppose",
    "option", "orange", "orbit", "orchard", "order", "ordinary", "organ", "orient", "original",
    "orphan", "ostrich", "other", "outdoor", "outer", "output", "outside", "oval", "oven", "over",
    "own", "owner", "oxygen", "oyster", "ozone", "pact", "paddle", "page", "pair", "palace",
    "palm", "panda", "panel", "panic", "panther", "paper", "parade", "parent", "park", "parrot",
    "party", "pass", "patch", "path", "patient", "patrol", "pattern", "pause", "pave", "payment",
    "peace", "peanut", "pear", "peasant", "pelican", "pen", "penalty", "pencil", "people",
    "pepper", "perfect", "permit", "person", "pet", "phone", "photo", "phrase", "physical",
    "piano", "picnic", "picture", "piece", "pig", "pigeon", "pill", "pilot", "pink", "pioneer",
    "pipe", "pistol", "pitch", "pizza", "place", "planet", "plastic", "plate", "play", "please",
    "pledge", "pluck", "plug", "plunge", "poem", "poet", "point", "polar", "pole", "police",
    "pond", "pony", "pool", "popular", "portion", "position", "possible", "post", "potato",
    "pottery", "poverty", "powder", "power", "practice", "praise", "predict", "prefer", "prepare",
    "present", "pretty", "prevent", "price", "pride", "primary", "print", "priority", "prison",
    "private", "prize", "problem", "process", "produce", "profit", "program", "project", "promote",
    "proof", "property", "prosper", "protect", "proud", "provide", "public", "pudding", "pull",
    "pulp", "pulse", "pumpkin", "punch", "pupil", "puppy", "purchase", "purity", "purpose",
    "purse", "push", "put", "puzzle", "pyramid", "quality", "quantum", "quarter", "question",
    "quick", "quit", "quiz", "quote", "rabbit", "raccoon", "race", "rack", "radar", "radio",
    "rail", "rain", "raise", "rally", "ramp", "ranch", "random", "range", "rapid", "rare", "rate",
    "rather", "raven", "raw", "razor", "ready", "real", "reason", "rebel", "rebuild", "recall",
    "receive", "recipe", "record", "recycle", "reduce", "reflect", "reform", "refuse", "region",
    "regret", "regular", "reject", "relax", "release", "relief", "rely", "remain", "remember",
    "remind", "remove", "render", "renew", "rent", "reopen", "repair", "repeat", "replace",
    "report", "require", "rescue", "resemble", "resist", "resource", "response", "result",
    "retire", "retreat", "return", "reunion", "reveal", "review", "reward", "rhythm", "rib",
    "ribbon", "rice", "rich", "ride", "ridge", "rifle", "right", "rigid", "ring", "riot", "ripple",
    "risk", "ritual", "rival", "river", "road", "roast", "robot", "robust", "rocket", "romance",
    "roof", "rookie", "room", "rose", "rotate", "rough", "round", "route", "royal", "rubber",
    "rude", "rug", "rule", "run", "runway", "rural", "sad", "saddle", "sadness", "safe", "sail",
    "salad", "salmon", "salon", "salt", "salute", "same", "sample", "sand", "satisfy", "satoshi",
    "sauce", "sausage", "save", "say", "scale", "scan", "scare", "scatter", "scene", "scheme",
    "school", "science", "scissors", "scorpion", "scout", "scrap", "screen", "script", "scrub",
    "sea", "search", "season", "seat", "second", "secret", "section", "security", "seed", "seek",
    "segment", "select", "sell", "seminar", "senior", "sense", "sentence", "series", "service",
    "session", "settle", "setup", "seven", "shadow", "shaft", "shallow", "share", "shed", "shell",
    "sheriff", "shield", "shift", "shine", "ship", "shiver", "shock", "shoe", "shoot", "shop",
    "short", "shoulder", "shove", "shrimp", "shrug", "shuffle", "shy", "sibling", "sick", "side",
    "siege", "sight", "sign", "silent", "silk", "silly", "silver", "similar", "simple", "since",
    "sing", "siren", "sister", "situate", "six", "size", "skate", "sketch", "ski", "skill", "skin",
    "skirt", "skull", "slab", "slam", "sleep", "slender", "slice", "slide", "slight", "slim",
    "slogan", "slot", "slow", "slush", "small", "smart", "smile", "smoke", "smooth", "snack",
    "snake", "snap", "sniff", "snow", "soap", "soccer", "social", "sock", "soda", "soft", "solar",
    "soldier", "solid", "solution", "solve", "someone", "song", "soon", "sorry", "sort", "soul",
    "sound", "soup", "source", "south", "space", "spare", "spatial", "spawn", "speak", "special",
    "speed", "spell", "spend", "sphere", "spice", "spider", "spike", "spin", "spirit", "split",
    "spoil", "sponsor", "spoon", "sport", "spot", "spray", "spread", "spring", "spy", "square",
    "squeeze", "squirrel", "stable", "stadium", "staff", "stage", "stairs", "stamp", "stand",
    "start", "state", "stay", "steak", "steel", "stem", "step", "stereo", "stick", "still",
    "sting", "stock", "stomach", "stone", "stool", "story", "stove", "strategy", "street",
    "strike", "strong", "struggle", "student", "stuff", "stumble", "style", "subject", "submit",
    "subway", "success", "such", "sudden", "suffer", "sugar", "suggest", "suit", "summer", "sun",
    "sunny", "sunset", "super", "supply", "supreme", "sure", "surface", "surge", "surprise",
    "surround", "survey", "suspect", "sustain", "swallow", "swamp", "swap", "swarm", "swear",
    "sweet", "swift", "swim", "swing", "switch", "sword", "symbol", "symptom", "syrup", "system",
    "table", "tackle", "tag", "tail", "talent", "talk", "tank", "tape", "target", "task", "taste",
    "tattoo", "taxi", "teach", "team", "tell", "ten", "tenant", "tennis", "tent", "term", "test",
    "text", "thank", "that", "theme", "then", "theory", "there", "they", "thing", "this",
    "thought", "three", "thrive", "throw", "thumb", "thunder", "ticket", "tide", "tiger", "tilt",
    "timber", "time", "tiny", "tip", "tired", "tissue", "title", "toast", "tobacco", "today",
    "toddler", "toe", "together", "toilet", "token", "tomato", "tomorrow", "tone", "tongue",
    "tonight", "tool", "tooth", "top", "topic", "topple", "torch", "tornado", "tortoise", "toss",
    "total", "tourist", "toward", "tower", "town", "toy", "track", "trade", "traffic", "tragic",
    "train", "transfer", "trap", "trash", "travel", "tray", "treat", "tree", "trend", "trial",
    "tribe", "trick", "trigger", "trim", "trip", "trophy", "trouble", "truck", "true", "truly",
    "trumpet", "trust", "truth", "try", "tube", "tuition", "tumble", "tuna", "tunnel", "turkey",
    "turn", "turtle", "twelve", "twenty", "twice", "twin", "twist", "two", "type", "typical",
    "ugly", "umbrella", "unable", "unaware", "uncle", "uncover", "under", "undo", "unfair",
    "unfold", "unhappy", "uniform", "unique", "unit", "universe", "unknown", "unlock", "until",
    "unusual", "unveil", "update", "upgrade", "uphold", "upon", "upper", "upset", "urban", "urge",
    "usage", "use", "used", "useful", "useless", "usual", "utility", "vacant", "vacuum", "vague",
    "valid", "valley", "valve", "van", "vanish", "vapor", "various", "vast", "vault", "vehicle",
    "velvet", "vendor", "venture", "venue", "verb", "verify", "version", "very", "vessel",
    "veteran", "viable", "vibrant", "vicious", "victory", "video", "view", "village", "vintage",
    "violin", "virtual", "virus", "visa", "visit", "visual", "vital", "vivid", "vocal", "voice",
    "void", "volcano", "volume", "vote", "voyage", "wage", "wagon", "wait", "walk", "wall",
    "walnut", "want", "warfare", "warm", "warrior", "wash", "wasp", "waste", "water", "wave",
    "way", "wealth", "weapon", "wear", "weasel", "weather", "web", "wedding", "weekend", "weird",
    "welcome", "west", "wet", "whale", "what", "wheat", "wheel", "when", "where", "whip",
    "whisper", "wide", "width", "wife", "wild", "will", "win", "window", "wine", "wing", "wink",
    "winner", "winter", "wire", "wisdom", "wise", "wish", "witness", "wolf", "woman", "wonder",
    "wood", "wool", "word", "work", "world", "worry", "worth", "wrap", "wreck", "wrestle", "wrist",
    "write", "wrong", "yard", "year", "yellow", "you", "young", "youth", "zebra", "zero", "zone",
    "zoo",
];

/// Parsed LAN short pairing code (`<nameplate>-<word>-<word>`, design 0052 D1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PairCode {
    pub nameplate: u16,
    pub word1: &'static str,
    pub word2: &'static str,
}

impl fmt::Display for PairCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.nameplate, self.word1, self.word2)
    }
}

/// Why a short pairing code string was refused. Never retains or echoes secrets or raw inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairCodeError {
    LooksLikeSecretToken,
    InvalidFormat,
    UnknownWord {
        word: String,
        suggestion: &'static str,
    },
}

impl fmt::Display for PairCodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LooksLikeSecretToken => f.write_str(
                "this looks like a join token or invite rather than a short pairing code; pipe it on standard input to `oxideswarm join` without passing it as an argument",
            ),
            Self::InvalidFormat => f.write_str(
                "invalid pairing code format: expected <number>-<word>-<word> (number 1..=999, two English words), for example 7-rocket-violin",
            ),
            Self::UnknownWord { word, suggestion } => write!(
                f,
                "'{word}' is not in the pairing word list (did you mean '{suggestion}'?)"
            ),
        }
    }
}

impl std::error::Error for PairCodeError {}

/// Returns `true` if `word` (case-insensitive ASCII, trimmed) belongs to [`WORDS`].
pub fn contains_word(word: &str) -> bool {
    lookup_word(word.trim()).is_some()
}

fn lookup_word(word: &str) -> Option<&'static str> {
    if word.is_empty() || word.len() > 16 || !word.bytes().all(|b| b.is_ascii_alphabetic()) {
        return None;
    }
    let lower = word.to_ascii_lowercase();
    WORDS
        .binary_search(&lower.as_str())
        .ok()
        .map(|idx| WORDS[idx])
}

/// Picks `N` uniformly random words from [`WORDS`] using [`SystemRandom`] without modulo bias
/// (`2048 = 2^11`, so masking 11 random bits is exact).
pub fn random_words<const N: usize>() -> GridResult<[&'static str; N]> {
    let rng = SystemRandom::new();
    let mut out = [""; N];
    for slot in &mut out {
        let mut buf = [0u8; 2];
        rng.fill(&mut buf)
            .map_err(|_| GridError::Config("system CSPRNG failed while generating words".into()))?;
        let idx = (u16::from_le_bytes(buf) & 0x07ff) as usize;
        *slot = WORDS[idx];
    }
    Ok(out)
}

/// Picks a uniformly random nameplate in `1..=999` using [`SystemRandom`] with rejection sampling.
pub fn random_nameplate() -> GridResult<u16> {
    let rng = SystemRandom::new();
    let mut buf = [0u8; 2];
    loop {
        rng.fill(&mut buf).map_err(|_| {
            GridError::Config("system CSPRNG failed while generating pairing nameplate".into())
        })?;
        let candidate = u16::from_le_bytes(buf) & 0x03ff;
        if (MIN_NAMEPLATE..=MAX_NAMEPLATE).contains(&candidate) {
            return Ok(candidate);
        }
    }
}

/// Returns the closest word in [`WORDS`] to `input` using restricted Damerau-Levenshtein
/// distance, breaking ties by longest common prefix and then alphabetical order.
pub fn closest_word(input: &str) -> &'static str {
    let query = input.trim().to_ascii_lowercase();
    let qb = query.as_bytes();
    let mut best_word = WORDS[0];
    let mut best_dist = usize::MAX;
    let mut best_lcp = 0usize;

    for &candidate in &WORDS {
        let cb = candidate.as_bytes();
        let dist = damerau_levenshtein(qb, cb);
        let lcp = qb.iter().zip(cb.iter()).take_while(|(a, b)| a == b).count();
        if dist < best_dist || (dist == best_dist && lcp > best_lcp) {
            best_dist = dist;
            best_lcp = lcp;
            best_word = candidate;
        }
    }
    best_word
}

/// Restricted Damerau-Levenshtein (Optimal String Alignment) distance over byte slices.
fn damerau_levenshtein(a: &[u8], b: &[u8]) -> usize {
    let m = a.len();
    let n = b.len();
    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }
    let cols = n + 1;
    let mut dp = vec![0usize; (m + 1) * cols];
    for (i, cell) in dp.iter_mut().step_by(cols).enumerate().take(m + 1) {
        *cell = i;
    }
    for j in 0..=n {
        dp[j] = j;
    }
    for i in 1..=m {
        for j in 1..=n {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let del = dp[(i - 1) * cols + j] + 1;
            let ins = dp[i * cols + (j - 1)] + 1;
            let sub = dp[(i - 1) * cols + (j - 1)] + cost;
            let mut best = del.min(ins).min(sub);
            if i >= 2 && j >= 2 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                let trans = dp[(i - 2) * cols + (j - 2)] + 1;
                best = best.min(trans);
            }
            dp[i * cols + j] = best;
        }
    }
    dp[m * cols + n]
}

impl PairCode {
    /// Generates a fresh random pairing code `<nameplate>-<word1>-<word2>`.
    pub fn generate() -> GridResult<Self> {
        let nameplate = random_nameplate()?;
        let [word1, word2] = random_words::<2>()?;
        Ok(Self {
            nameplate,
            word1,
            word2,
        })
    }

    /// Parses `<nameplate>-<word1>-<word2>` and validates both words against [`WORDS`] without
    /// echoing secrets or malformed inputs.
    pub fn parse(raw: &str) -> Result<Self, PairCodeError> {
        let trimmed = raw.trim();
        let lower_prefix = trimmed.to_ascii_lowercase();
        if lower_prefix.starts_with("oxs1-") || lower_prefix.starts_with("oxticket") {
            return Err(PairCodeError::LooksLikeSecretToken);
        }
        if trimmed.len() > 64 {
            return Err(PairCodeError::InvalidFormat);
        }

        let mut parts = trimmed.split('-');
        let Some(p0) = parts.next() else {
            return Err(PairCodeError::InvalidFormat);
        };
        let Some(p1) = parts.next() else {
            return Err(PairCodeError::InvalidFormat);
        };
        let Some(p2) = parts.next() else {
            return Err(PairCodeError::InvalidFormat);
        };
        if parts.next().is_some() {
            return Err(PairCodeError::InvalidFormat);
        }

        if p0.is_empty()
            || p0.len() > 3
            || p0.starts_with('0')
            || !p0.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(PairCodeError::InvalidFormat);
        }
        let Ok(nameplate) = p0.parse::<u16>() else {
            return Err(PairCodeError::InvalidFormat);
        };
        if !(MIN_NAMEPLATE..=MAX_NAMEPLATE).contains(&nameplate) {
            return Err(PairCodeError::InvalidFormat);
        }

        let word1 = validate_word_part(p1)?;
        let word2 = validate_word_part(p2)?;
        Ok(Self {
            nameplate,
            word1,
            word2,
        })
    }

    /// Returns the SPAKE2 password bytes (`oxs-pair-v1:<word1>-<word2>`).
    pub fn spake2_password(&self) -> String {
        format!("oxs-pair-v1:{}-{}", self.word1, self.word2)
    }
}

fn validate_word_part(raw: &str) -> Result<&'static str, PairCodeError> {
    if raw.is_empty() || raw.len() > 16 || !raw.bytes().all(|b| b.is_ascii_alphabetic()) {
        return Err(PairCodeError::InvalidFormat);
    }
    let lower = raw.to_ascii_lowercase();
    if let Some(found) = lookup_word(&lower) {
        Ok(found)
    } else {
        let suggestion = closest_word(&lower);
        Err(PairCodeError::UnknownWord {
            word: lower,
            suggestion,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wordlist_is_bip39_2048_sorted_lowercase_ascii() {
        assert_eq!(WORDS.len(), 2048);
        for w in &WORDS {
            assert!((3..=8).contains(&w.len()), "unexpected word length: {w}");
            assert!(
                w.bytes().all(|b| b.is_ascii_lowercase()),
                "word not lowercase ASCII: {w}"
            );
        }
        for pair in WORDS.windows(2) {
            assert!(pair[0] < pair[1], "words not strictly sorted: {pair:?}");
        }
        assert!(contains_word("rocket"));
        assert!(contains_word("violin"));
        assert!(contains_word("  Rocket "));
        assert!(!contains_word("notaword"));
    }

    #[test]
    fn closest_word_suggests_typos() {
        assert_eq!(closest_word("rokcet"), "rocket");
        assert_eq!(closest_word("voilin"), "violin");
        assert_eq!(closest_word("abandn"), "abandon");
    }

    #[test]
    fn pair_code_grammar_and_suggestions() {
        let valid = PairCode::parse(" 7-Rocket-VIOLIN ").unwrap();
        assert_eq!(valid.nameplate, 7);
        assert_eq!(valid.word1, "rocket");
        assert_eq!(valid.word2, "violin");
        assert_eq!(valid.to_string(), "7-rocket-violin");
        assert_eq!(valid.spake2_password(), "oxs-pair-v1:rocket-violin");

        assert!(PairCode::parse("1-abandon-zoo").is_ok());
        assert!(PairCode::parse("999-abandon-zoo").is_ok());

        for bad in [
            "0-rocket-violin",
            "07-rocket-violin",
            "1000-rocket-violin",
            "-7-rocket-violin",
            "7-rocket",
            "7-rocket-violin-extra",
            "7-rocket1-violin",
            "",
        ] {
            assert_eq!(
                PairCode::parse(bad),
                Err(PairCodeError::InvalidFormat),
                "{bad}"
            );
        }

        let err1 = PairCode::parse("7-rokcet-violin").unwrap_err();
        assert_eq!(
            err1,
            PairCodeError::UnknownWord {
                word: "rokcet".into(),
                suggestion: "rocket",
            }
        );
        assert!(err1.to_string().contains("did you mean 'rocket'?"));

        let err2 = PairCode::parse("7-rocket-voilin").unwrap_err();
        assert!(err2.to_string().contains("did you mean 'violin'?"));
    }

    #[test]
    fn non_echo_refusal_of_tokens_and_hex_secrets() {
        let hex_secret = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
        let invite_tok = "oxs1-invite:eyJtIjoiMTI3LjAuMC4xOjgwODgifQ.0123456789abcdef";
        let join_tok = "oxs1-join:eyJzIjoiYWIifQ.0123456789abcdef";
        let ticket = "oxticket1supersecretvalue";

        for sensitive in [hex_secret, invite_tok, join_tok, ticket] {
            let err = PairCode::parse(sensitive).unwrap_err();
            let display = err.to_string();
            let dbg = format!("{err:?}");
            assert!(
                !display.contains(sensitive),
                "display echoed secret: {display}"
            );
            assert!(!dbg.contains(sensitive), "debug echoed secret: {dbg}");
        }

        assert_eq!(
            PairCode::parse(invite_tok).unwrap_err(),
            PairCodeError::LooksLikeSecretToken
        );
        assert_eq!(
            PairCode::parse(join_tok).unwrap_err(),
            PairCodeError::LooksLikeSecretToken
        );
        assert_eq!(
            PairCode::parse(hex_secret).unwrap_err(),
            PairCodeError::InvalidFormat
        );
    }

    #[test]
    fn random_code_generation_respects_bounds() {
        for _ in 0..32 {
            let code = PairCode::generate().unwrap();
            assert!((MIN_NAMEPLATE..=MAX_NAMEPLATE).contains(&code.nameplate));
            assert!(contains_word(code.word1));
            assert!(contains_word(code.word2));
        }
    }
}
