import os
import logging
import json
import re
import subprocess
from glob import glob
from Authentication_BlobStorageClient import BlobStorageClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv("app.env")

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants from ENV
FFMPEG_PATH = r"C:\ffmpeg\bin\ffmpeg.exe"
CHUNK_DURATION = 30
COGNITIVE_API_KEY = os.getenv("SUBSCRIPTION_COGNITIVE_SPEACH_TEXT_KEY")
COGNITIVE_ENDPOINT = os.getenv("ENDPOINT_COGNITIVE_SPEACH_TEXT")
OUTPUT_CONTAINER = os.getenv("OUTPUT_CONTAINER")

CHUNK_OUTPUT_DIR = os.getcwd()

def extract_audio(video_path, audio_path):
    try:
        command = [
            FFMPEG_PATH, "-y", "-i", video_path,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1", audio_path
        ]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except Exception as e:
        logging.error(f"❌ Audio extraction failed for {video_path}: {e}")
        return False

def audio_to_text_cognitive_services(audio_file_path):
    import base64
    import requests

    headers = {
        'Ocp-Apim-Subscription-Key': COGNITIVE_API_KEY,
        'Content-type': 'audio/wav; codecs=audio/pcm; samplerate=16000'
    }

    try:
        with open(audio_file_path, 'rb') as audio_file:
            audio_data = audio_file.read()

        response = requests.post(COGNITIVE_ENDPOINT, headers=headers, data=audio_data)
        if response.status_code != 200:
            logging.error(f"❌ STT error: {response.status_code} {response.text}")
            return None

        response_data = response.json()
        display_text = response_data.get("DisplayText", "")
        pron_assessment_params = {
            "ReferenceText": display_text,
            "GradingSystem": "HundredMark",
            "Granularity": "Word",
            "Dimension": "Comprehensive"
        }

        headers['Pronunciation-Assessment'] = base64.b64encode(json.dumps(pron_assessment_params).encode()).decode()
        response = requests.post(COGNITIVE_ENDPOINT, headers=headers, data=audio_data)
        return response.json() if response.status_code == 200 else None

    except Exception as e:
        logging.error(f"❌ Exception during STT: {e}")
        return None

def convert_word_timings_to_seconds(response_data, chunk_name):
    def format_hh_mm_ss_ms(ms_total):
        hours = ms_total // (60 * 60 * 1000)
        minutes = (ms_total % (60 * 60 * 1000)) // (60 * 1000)
        seconds = (ms_total % (60 * 1000)) // 1000
        milliseconds = ms_total % 1000
        return f"{hours:02}:{minutes:02}:{seconds:02}:{milliseconds:03}"

    # ✅ Static dictionary mapping clip IDs to offsets in seconds
    clip_offset_seconds = {
            "clip-001": 30, "clip-002": 60, "clip-003": 90, "clip-004": 120, "clip-005": 150,
            "clip-006": 180, "clip-007": 210, "clip-008": 240, "clip-009": 270, "clip-010": 300,
            "clip-011": 330, "clip-012": 360, "clip-013": 390, "clip-014": 420, "clip-015": 450,
            "clip-016": 480, "clip-017": 510, "clip-018": 540, "clip-019": 570, "clip-020": 600,
            "clip-021": 630, "clip-022": 660, "clip-023": 690, "clip-024": 720, "clip-025": 750,
            "clip-026": 780, "clip-027": 810, "clip-028": 840, "clip-029": 870, "clip-030": 900,
            "clip-031": 930, "clip-032": 960, "clip-033": 990, "clip-034": 1020, "clip-035": 1050,
            "clip-036": 1080, "clip-037": 1110, "clip-038": 1140, "clip-039": 1170, "clip-040": 1200,
            "clip-041": 1230, "clip-042": 1260, "clip-043": 1290, "clip-044": 1320, "clip-045": 1350,
            "clip-046": 1380, "clip-047": 1410, "clip-048": 1440, "clip-049": 1470, "clip-050": 1500,
            "clip-051": 1530, "clip-052": 1560, "clip-053": 1590, "clip-054": 1620, "clip-055": 1650,
            "clip-056": 1680, "clip-057": 1710, "clip-058": 1740, "clip-059": 1770, "clip-060": 1800,
            "clip-061": 1830, "clip-062": 1860, "clip-063": 1890, "clip-064": 1920, "clip-065": 1950,
            "clip-066": 1980, "clip-067": 2010, "clip-068": 2040, "clip-069": 2070, "clip-070": 2100,
            "clip-071": 2130, "clip-072": 2160, "clip-073": 2190, "clip-074": 2220, "clip-075": 2250,
            "clip-076": 2280, "clip-077": 2310, "clip-078": 2340, "clip-079": 2370, "clip-080": 2400,
            "clip-081": 2430, "clip-082": 2460, "clip-083": 2490, "clip-084": 2520, "clip-085": 2550,
            "clip-086": 2580, "clip-087": 2610, "clip-088": 2640, "clip-089": 2670, "clip-090": 2700,
            "clip-091": 2730, "clip-092": 2760, "clip-093": 2790, "clip-094": 2820, "clip-095": 2850,
            "clip-096": 2880, "clip-097": 2910, "clip-098": 2940, "clip-099": 2970, "clip-100": 3000,
            "clip-101": 3030, "clip-102": 3060, "clip-103": 3090, "clip-104": 3120, "clip-105": 3150,
            "clip-106": 3180, "clip-107": 3210, "clip-108": 3240, "clip-109": 3270, "clip-110": 3300,
            "clip-111": 3330, "clip-112": 3360, "clip-113": 3390, "clip-114": 3420, "clip-115": 3450,
            "clip-116": 3480, "clip-117": 3510, "clip-118": 3540, "clip-119": 3570, "clip-120": 3600,
            "clip-121": 3630, "clip-122": 3660, "clip-123": 3690, "clip-124": 3720, "clip-125": 3750,
            "clip-126": 3780, "clip-127": 3810, "clip-128": 3840, "clip-129": 3870, "clip-130": 3900,
            "clip-131": 3930, "clip-132": 3960, "clip-133": 3990, "clip-134": 4020, "clip-135": 4050,
            "clip-136": 4080, "clip-137": 4110, "clip-138": 4140, "clip-139": 4170, "clip-140": 4200,
            "clip-141": 4230, "clip-142": 4260, "clip-143": 4290, "clip-144": 4320, "clip-145": 4350,
            "clip-146": 4380, "clip-147": 4410, "clip-148": 4440, "clip-149": 4470, "clip-150": 4500,
            "clip-151": 4530, "clip-152": 4560, "clip-153": 4590, "clip-154": 4620, "clip-155": 4650,
            "clip-156": 4680, "clip-157": 4710, "clip-158": 4740, "clip-159": 4770, "clip-160": 4800,
            "clip-161": 4830, "clip-162": 4860, "clip-163": 4890, "clip-164": 4920, "clip-165": 4950,
            "clip-166": 4980, "clip-167": 5010, "clip-168": 5040, "clip-169": 5070, "clip-170": 5100,
            "clip-171": 5130, "clip-172": 5160, "clip-173": 5190, "clip-174": 5220, "clip-175": 5250,
            "clip-176": 5280, "clip-177": 5310, "clip-178": 5340, "clip-179": 5370, "clip-180": 5400,
            "clip-181": 5430, "clip-182": 5460, "clip-183": 5490, "clip-184": 5520, "clip-185": 5550,
            "clip-186": 5580, "clip-187": 5610, "clip-188": 5640, "clip-189": 5670, "clip-190": 5700,
            "clip-191": 5730, "clip-192": 5760, "clip-193": 5790, "clip-194": 5820, "clip-195": 5850,
            "clip-196": 5880, "clip-197": 5910, "clip-198": 5940, "clip-199": 5970, "clip-200": 6000,
            "clip-201": 6030, "clip-202": 6060, "clip-203": 6090, "clip-204": 6120, "clip-205": 6150,
            "clip-206": 6180, "clip-207": 6210, "clip-208": 6240, "clip-209": 6270, "clip-210": 6300,
            "clip-211": 6330, "clip-212": 6360, "clip-213": 6390, "clip-214": 6420, "clip-215": 6450,
            "clip-216": 6480, "clip-217": 6510, "clip-218": 6540, "clip-219": 6570, "clip-220": 6600,
            "clip-221": 6630, "clip-222": 6660, "clip-223": 6690, "clip-224": 6720, "clip-225": 6750,
            "clip-226": 6780, "clip-227": 6810, "clip-228": 6840, "clip-229": 6870, "clip-230": 6900,
            "clip-231": 6930, "clip-232": 6960, "clip-233": 6990, "clip-234": 7020, "clip-235": 7050,
            "clip-236": 7080, "clip-237": 7110, "clip-238": 7140, "clip-239": 7170, "clip-240": 7200,
            "clip-241": 7230, "clip-242": 7260, "clip-243": 7290, "clip-244": 7320, "clip-245": 7350,
            "clip-246": 7380, "clip-247": 7410, "clip-248": 7440, "clip-249": 7470, "clip-250": 7500,
            "clip-251": 7530, "clip-252": 7560, "clip-253": 7590, "clip-254": 7620, "clip-255": 7650,
            "clip-256": 7680, "clip-257": 7710, "clip-258": 7740, "clip-259": 7770, "clip-260": 7800,
            "clip-261": 7830, "clip-262": 7860, "clip-263": 7890, "clip-264": 7920, "clip-265": 7950,
            "clip-266": 7980, "clip-267": 8010, "clip-268": 8040, "clip-269": 8070, "clip-270": 8100,
            "clip-271": 8130, "clip-272": 8160, "clip-273": 8190, "clip-274": 8220, "clip-275": 8250,
            "clip-276": 8280, "clip-277": 8310, "clip-278": 8340, "clip-279": 8370, "clip-280": 8400,
            "clip-281": 8430, "clip-282": 8460, "clip-283": 8490, "clip-284": 8520, "clip-285": 8550,
            "clip-286": 8580, "clip-287": 8610, "clip-288": 8640, "clip-289": 8670, "clip-290": 8700,
            "clip-291": 8730, "clip-292": 8760, "clip-293": 8790, "clip-294": 8820, "clip-295": 8850,
            "clip-296": 8880, "clip-297": 8910, "clip-298": 8940, "clip-299": 8970, "clip-300": 9000,
            "clip-301": 9030, "clip-302": 9060, "clip-303": 9090, "clip-304": 9120, "clip-305": 9150,
            "clip-306": 9180, "clip-307": 9210, "clip-308": 9240, "clip-309": 9270, "clip-310": 9300,
            "clip-311": 9330, "clip-312": 9360, "clip-313": 9390, "clip-314": 9420, "clip-315": 9450,
            "clip-316": 9480, "clip-317": 9510, "clip-318": 9540, "clip-319": 9570, "clip-320": 9600,
            "clip-321": 9630, "clip-322": 9660, "clip-323": 9690, "clip-324": 9720, "clip-325": 9750,
            "clip-326": 9780, "clip-327": 9810, "clip-328": 9840, "clip-329": 9870, "clip-330": 9900,
            "clip-331": 9930, "clip-332": 9960, "clip-333": 9990, "clip-334": 10020, "clip-335": 10050,
            "clip-336": 10080, "clip-337": 10110, "clip-338": 10140, "clip-339": 10170, "clip-340": 10200,
            "clip-341": 10230, "clip-342": 10260, "clip-343": 10290, "clip-344": 10320, "clip-345": 10350,
            "clip-346": 10380, "clip-347": 10410, "clip-348": 10440, "clip-349": 10470, "clip-350": 10500,
            "clip-351": 10530, "clip-352": 10560, "clip-353": 10590, "clip-354": 10620, "clip-355": 10650,
            "clip-356": 10680, "clip-357": 10710, "clip-358": 10740, "clip-359": 10770, "clip-360": 10800,
            "clip-361": 10830, "clip-362": 10860, "clip-363": 10890, "clip-364": 10920, "clip-365": 10950,
            "clip-366": 10980, "clip-367": 11010, "clip-368": 11040, "clip-369": 11070, "clip-370": 11100,
            "clip-371": 11130, "clip-372": 11160, "clip-373": 11190, "clip-374": 11220, "clip-375": 11250,
            "clip-376": 11280, "clip-377": 11310, "clip-378": 11340, "clip-379": 11370, "clip-380": 11400,
            "clip-381": 11430, "clip-382": 11460, "clip-383": 11490, "clip-384": 11520, "clip-385": 11550,
            "clip-386": 11580, "clip-387": 11610, "clip-388": 11640, "clip-389": 11670, "clip-390": 11700,
            "clip-391": 11730, "clip-392": 11760, "clip-393": 11790, "clip-394": 11820, "clip-395": 11850,
            "clip-396": 11880, "clip-397": 11910, "clip-398": 11940, "clip-399": 11970, "clip-400": 12000,
            "clip-401": 12030, "clip-402": 12060, "clip-403": 12090, "clip-404": 12120, "clip-405": 12150,
            "clip-406": 12180, "clip-407": 12210, "clip-408": 12240, "clip-409": 12270, "clip-410": 12300,
            "clip-411": 12330, "clip-412": 12360, "clip-413": 12390, "clip-414": 12420, "clip-415": 12450,
            "clip-416": 12480, "clip-417": 12510, "clip-418": 12540, "clip-419": 12570, "clip-420": 12600,
            "clip-421": 12630, "clip-422": 12660, "clip-423": 12690, "clip-424": 12720, "clip-425": 12750,
            "clip-426": 12780, "clip-427": 12810, "clip-428": 12840, "clip-429": 12870, "clip-430": 12900,
            "clip-431": 12930, "clip-432": 12960, "clip-433": 12990, "clip-434": 13020, "clip-435": 13050,
            "clip-436": 13080, "clip-437": 13110, "clip-438": 13140, "clip-439": 13170, "clip-440": 13200,
            "clip-441": 13230, "clip-442": 13260, "clip-443": 13290, "clip-444": 13320, "clip-445": 13350,
            "clip-446": 13380, "clip-447": 13410, "clip-448": 13440, "clip-449": 13470, "clip-450": 13500,
            "clip-451": 13530, "clip-452": 13560, "clip-453": 13590, "clip-454": 13620, "clip-455": 13650,
            "clip-456": 13680, "clip-457": 13710, "clip-458": 13740, "clip-459": 13770, "clip-460": 13800,
            "clip-461": 13830, "clip-462": 13860, "clip-463": 13890, "clip-464": 13920, "clip-465": 13950,
            "clip-466": 13980, "clip-467": 14010, "clip-468": 14040, "clip-469": 14070, "clip-470": 14100,
            "clip-471": 14130, "clip-472": 14160, "clip-473": 14190, "clip-474": 14220, "clip-475": 14250,
            "clip-476": 14280, "clip-477": 14310, "clip-478": 14340, "clip-479": 14370, "clip-480": 14400,
            "clip-481": 14430, "clip-482": 14460, "clip-483": 14490, "clip-484": 14520, "clip-485": 14550,
            "clip-486": 14580, "clip-487": 14610, "clip-488": 14640, "clip-489": 14670, "clip-490": 14700,
            "clip-491": 14730, "clip-492": 14760, "clip-493": 14790, "clip-494": 14820, "clip-495": 14850,
            "clip-496": 14880, "clip-497": 14910, "clip-498": 14940, "clip-499": 14970, "clip-500": 15000,
            "clip-501": 15030, "clip-502": 15060, "clip-503": 15090, "clip-504": 15120, "clip-505": 15150,
            "clip-506": 15180, "clip-507": 15210, "clip-508": 15240, "clip-509": 15270, "clip-510": 15300,
            "clip-511": 15330, "clip-512": 15360, "clip-513": 15390, "clip-514": 15420, "clip-515": 15450,
            "clip-516": 15480, "clip-517": 15510, "clip-518": 15540, "clip-519": 15570, "clip-520": 15600,
            "clip-521": 15630, "clip-522": 15660, "clip-523": 15690, "clip-524": 15720, "clip-525": 15750,
            "clip-526": 15780, "clip-527": 15810, "clip-528": 15840, "clip-529": 15870, "clip-530": 15900,
            "clip-531": 15930, "clip-532": 15960, "clip-533": 15990, "clip-534": 16020, "clip-535": 16050,
            "clip-536": 16080, "clip-537": 16110, "clip-538": 16140, "clip-539": 16170, "clip-540": 16200,
            "clip-541": 16230, "clip-542": 16260, "clip-543": 16290, "clip-544": 16320, "clip-545": 16350,
            "clip-546": 16380, "clip-547": 16410, "clip-548": 16440, "clip-549": 16470, "clip-550": 16500,
            "clip-551": 16530, "clip-552": 16560, "clip-553": 16590, "clip-554": 16620, "clip-555": 16650,
            "clip-556": 16680, "clip-557": 16710, "clip-558": 16740, "clip-559": 16770, "clip-560": 16800,
            "clip-561": 16830, "clip-562": 16860, "clip-563": 16890, "clip-564": 16920, "clip-565": 16950,
            "clip-566": 16980, "clip-567": 17010, "clip-568": 17040, "clip-569": 17070, "clip-570": 17100,
            "clip-571": 17130, "clip-572": 17160, "clip-573": 17190, "clip-574": 17220, "clip-575": 17250,
            "clip-576": 17280, "clip-577": 17310, "clip-578": 17340, "clip-579": 17370, "clip-580": 17400,
            "clip-581": 17430, "clip-582": 17460, "clip-583": 17490, "clip-584": 17520, "clip-585": 17550,
            "clip-586": 17580, "clip-587": 17610, "clip-588": 17640, "clip-589": 17670, "clip-590": 17700,
            "clip-591": 17730, "clip-592": 17760, "clip-593": 17790, "clip-594": 17820, "clip-595": 17850,
            "clip-596": 17880, "clip-597": 17910, "clip-598": 17940, "clip-599": 17970, "clip-600": 18000,
            "clip-601": 18030, "clip-602": 18060, "clip-603": 18090, "clip-604": 18120, "clip-605": 18150,
            "clip-606": 18180, "clip-607": 18210, "clip-608": 18240, "clip-609": 18270, "clip-610": 18300,
            "clip-611": 18330, "clip-612": 18360, "clip-613": 18390, "clip-614": 18420, "clip-615": 18450,
            "clip-616": 18480, "clip-617": 18510, "clip-618": 18540, "clip-619": 18570, "clip-620": 18600,
            "clip-621": 18630, "clip-622": 18660, "clip-623": 18690, "clip-624": 18720, "clip-625": 18750,
            "clip-626": 18780, "clip-627": 18810, "clip-628": 18840, "clip-629": 18870, "clip-630": 18900,
            "clip-631": 18930, "clip-632": 18960, "clip-633": 18990, "clip-634": 19020, "clip-635": 19050,
            "clip-636": 19080, "clip-637": 19110, "clip-638": 19140, "clip-639": 19170, "clip-640": 19200,
            "clip-641": 19230, "clip-642": 19260, "clip-643": 19290, "clip-644": 19320, "clip-645": 19350,
            "clip-646": 19380, "clip-647": 19410, "clip-648": 19440, "clip-649": 19470, "clip-650": 19500,
            "clip-651": 19530, "clip-652": 19560, "clip-653": 19590, "clip-654": 19620, "clip-655": 19650,
            "clip-656": 19680, "clip-657": 19710, "clip-658": 19740, "clip-659": 19770, "clip-660": 19800,
            "clip-661": 19830, "clip-662": 19860, "clip-663": 19890, "clip-664": 19920, "clip-665": 19950,
            "clip-666": 19980, "clip-667": 20010, "clip-668": 20040, "clip-669": 20070, "clip-670": 20100,
            "clip-671": 20130, "clip-672": 20160, "clip-673": 20190, "clip-674": 20220, "clip-675": 20250,
            "clip-676": 20280, "clip-677": 20310, "clip-678": 20340, "clip-679": 20370, "clip-680": 20400,
            "clip-681": 20430, "clip-682": 20460, "clip-683": 20490, "clip-684": 20520, "clip-685": 20550,
            "clip-686": 20580, "clip-687": 20610, "clip-688": 20640, "clip-689": 20670, "clip-690": 20700,
            "clip-691": 20730, "clip-692": 20760, "clip-693": 20790, "clip-694": 20820, "clip-695": 20850,
            "clip-696": 20880, "clip-697": 20910, "clip-698": 20940, "clip-699": 20970, "clip-700": 21000,
            "clip-701": 21030, "clip-702": 21060, "clip-703": 21090, "clip-704": 21120, "clip-705": 21150,
            "clip-706": 21180, "clip-707": 21210, "clip-708": 21240, "clip-709": 21270, "clip-710": 21300,
            "clip-711": 21330, "clip-712": 21360, "clip-713": 21390, "clip-714": 21420, "clip-715": 21450,
            "clip-716": 21480, "clip-717": 21510, "clip-718": 21540, "clip-719": 21570, "clip-720": 21600,
            "clip-721": 21630, "clip-722": 21660, "clip-723": 21690, "clip-724": 21720, "clip-725": 21750,
            "clip-726": 21780, "clip-727": 21810, "clip-728": 21840, "clip-729": 21870, "clip-730": 21900,
            "clip-731": 21930, "clip-732": 21960, "clip-733": 21990, "clip-734": 22020, "clip-735": 22050,
            "clip-736": 22080, "clip-737": 22110, "clip-738": 22140, "clip-739": 22170, "clip-740": 22200,
            "clip-741": 22230, "clip-742": 22260, "clip-743": 22290, "clip-744": 22320, "clip-745": 22350,
            "clip-746": 22380, "clip-747": 22410, "clip-748": 22440, "clip-749": 22470, "clip-750": 22500,
            "clip-751": 22530, "clip-752": 22560, "clip-753": 22590, "clip-754": 22620, "clip-755": 22650,
            "clip-756": 22680, "clip-757": 22710, "clip-758": 22740, "clip-759": 22770, "clip-760": 22800,
            "clip-761": 22830, "clip-762": 22860, "clip-763": 22890, "clip-764": 22920, "clip-765": 22950,
            "clip-766": 22980, "clip-767": 23010, "clip-768": 23040, "clip-769": 23070, "clip-770": 23100,
            "clip-771": 23130, "clip-772": 23160, "clip-773": 23190, "clip-774": 23220, "clip-775": 23250,
            "clip-776": 23280, "clip-777": 23310, "clip-778": 23340, "clip-779": 23370, "clip-780": 23400,
            "clip-781": 23430, "clip-782": 23460, "clip-783": 23490, "clip-784": 23520, "clip-785": 23550,
            "clip-786": 23580, "clip-787": 23610, "clip-788": 23640, "clip-789": 23670, "clip-790": 23700,
            "clip-791": 23730, "clip-792": 23760, "clip-793": 23790, "clip-794": 23820, "clip-795": 23850,
            "clip-796": 23880, "clip-797": 23910, "clip-798": 23940, "clip-799": 23970, "clip-800": 24000,
            "clip-801": 24030, "clip-802": 24060, "clip-803": 24090, "clip-804": 24120, "clip-805": 24150,
            "clip-806": 24180, "clip-807": 24210, "clip-808": 24240, "clip-809": 24270, "clip-810": 24300,
            "clip-811": 24330, "clip-812": 24360, "clip-813": 24390, "clip-814": 24420, "clip-815": 24450,
            "clip-816": 24480, "clip-817": 24510, "clip-818": 24540, "clip-819": 24570, "clip-820": 24600,
            "clip-821": 24630, "clip-822": 24660, "clip-823": 24690, "clip-824": 24720, "clip-825": 24750,
            "clip-826": 24780, "clip-827": 24810, "clip-828": 24840, "clip-829": 24870, "clip-830": 24900,
            "clip-831": 24930, "clip-832": 24960, "clip-833": 24990, "clip-834": 25020, "clip-835": 25050,
            "clip-836": 25080, "clip-837": 25110, "clip-838": 25140, "clip-839": 25170, "clip-840": 25200,
            "clip-841": 25230, "clip-842": 25260, "clip-843": 25290, "clip-844": 25320, "clip-845": 25350,
            "clip-846": 25380, "clip-847": 25410, "clip-848": 25440, "clip-849": 25470, "clip-850": 25500,
            "clip-851": 25530, "clip-852": 25560, "clip-853": 25590, "clip-854": 25620, "clip-855": 25650,
            "clip-856": 25680, "clip-857": 25710, "clip-858": 25740, "clip-859": 25770, "clip-860": 25800,
            "clip-861": 25830, "clip-862": 25860, "clip-863": 25890, "clip-864": 25920, "clip-865": 25950,
            "clip-866": 25980, "clip-867": 26010, "clip-868": 26040, "clip-869": 26070, "clip-870": 26100,
            "clip-871": 26130, "clip-872": 26160, "clip-873": 26190, "clip-874": 26220, "clip-875": 26250,
            "clip-876": 26280, "clip-877": 26310, "clip-878": 26340, "clip-879": 26370, "clip-880": 26400,
            "clip-881": 26430, "clip-882": 26460, "clip-883": 26490, "clip-884": 26520, "clip-885": 26550,
            "clip-886": 26580, "clip-887": 26610, "clip-888": 26640, "clip-889": 26670, "clip-890": 26700,
            "clip-891": 26730, "clip-892": 26760, "clip-893": 26790, "clip-894": 26820, "clip-895": 26850,
            "clip-896": 26880, "clip-897": 26910, "clip-898": 26940, "clip-899": 26970, "clip-900": 27000,
            "clip-901": 27030, "clip-902": 27060, "clip-903": 27090, "clip-904": 27120, "clip-905": 27150,
            "clip-906": 27180, "clip-907": 27210, "clip-908": 27240, "clip-909": 27270, "clip-910": 27300,
            "clip-911": 27330, "clip-912": 27360, "clip-913": 27390, "clip-914": 27420, "clip-915": 27450,
            "clip-916": 27480, "clip-917": 27510, "clip-918": 27540, "clip-919": 27570, "clip-920": 27600,
            "clip-921": 27630, "clip-922": 27660, "clip-923": 27690, "clip-924": 27720, "clip-925": 27750,
            "clip-926": 27780, "clip-927": 27810, "clip-928": 27840, "clip-929": 27870, "clip-930": 27900,
            "clip-931": 27930, "clip-932": 27960, "clip-933": 27990, "clip-934": 28020, "clip-935": 28050,
            "clip-936": 28080, "clip-937": 28110, "clip-938": 28140, "clip-939": 28170, "clip-940": 28200,
            "clip-941": 28230, "clip-942": 28260, "clip-943": 28290, "clip-944": 28320, "clip-945": 28350,
            "clip-946": 28380, "clip-947": 28410, "clip-948": 28440, "clip-949": 28470, "clip-950": 28500,
            "clip-951": 28530, "clip-952": 28560, "clip-953": 28590, "clip-954": 28620, "clip-955": 28650,
            "clip-956": 28680, "clip-957": 28710, "clip-958": 28740, "clip-959": 28770, "clip-960": 28800
        }

    # ✅ Extract "clip-XXX" identifier from chunk name
    match = re.search(r"(clip-\d{3})", chunk_name)
    clip_id = match.group(1) if match else None
    offset_ms = clip_offset_seconds.get(clip_id, 0) * 1000  # seconds → ms

    words = []
    try:
        word_entries = response_data.get("NBest", [{}])[0].get("Words", [])
        for word in word_entries:
            original_offset = word["Offset"]
            original_duration = word["Duration"]
            original_end = original_offset + original_duration

            # Shifted time based on dictionary offset
            start_ms = (original_offset // 10_000) + offset_ms
            end_ms = (original_end // 10_000) + offset_ms

            words.append({
                **word,
                "start_original": original_offset,
                "end_original": original_end,
                "word start time": format_hh_mm_ss_ms(start_ms),
                "word end time": format_hh_mm_ss_ms(end_ms)
            })
    except Exception as e:
        logging.warning(f"⚠️ Failed to convert word timings for {chunk_name}: {e}")
    return words

def split_video_into_chunks(video_source_path):
    logging.info("🎬 Splitting video into 30s chunks...")
    base_name = os.path.splitext(os.path.basename(video_source_path))[0]
    output_template = os.path.join(CHUNK_OUTPUT_DIR, f"{base_name}-clip-%03d.mp4")
    command = [
        FFMPEG_PATH, "-i", video_source_path,
        "-c", "copy", "-map", "0",
        "-f", "segment", "-segment_time", str(CHUNK_DURATION),
        output_template
    ]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return sorted(glob(os.path.join(CHUNK_OUTPUT_DIR, f"{base_name}-clip-*.mp4")))

async def process_all_video_chunks(video_source_path):
    storage = BlobStorageClient()
    chunk_paths = split_video_into_chunks(video_source_path)

    for chunk_path in chunk_paths:
        base_name = os.path.splitext(os.path.basename(chunk_path))[0]
        audio_path = os.path.join(CHUNK_OUTPUT_DIR, f"{base_name}.wav")

        logging.info(f"🎧 Processing {base_name}...")

        if not extract_audio(chunk_path, audio_path):
            continue

        response_data = audio_to_text_cognitive_services(audio_path)
        if not response_data:
            continue

        word_timings = convert_word_timings_to_seconds(response_data, base_name)

        json_blob_name = f"{base_name}_transcription.json"
        await storage.upload_blob(
            container_name=OUTPUT_CONTAINER,
            blob_name=json_blob_name,
            file_path_or_bytes=json.dumps(word_timings, indent=2).encode("utf-8")
        )

        os.remove(chunk_path)
        os.remove(audio_path)
        logging.info(f"🧹 Deleted: {chunk_path}, {audio_path}")

    logging.info("✅ All chunks processed and uploaded.")
