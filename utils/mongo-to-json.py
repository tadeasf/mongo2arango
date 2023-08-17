import subprocess
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import click
from tqdm import tqdm
import simplejson as json

# Load environment variables from .env file
load_dotenv()

# MongoDB connection details from .env
MONGODB_URI = os.getenv("MONGODB_URI")

COLLECTIONS_TO_EXPORT = [
    #    "assignationOfferClaims",
    #    "assignationOffers",
    #    "assignationOffers_report_view",
    #    "assignationPayouts",
    #    "assignations",
    #    "chargebacks",
    #    "compensations",
    "countries",
    "currencyRates",
    "customerFeedbacks",
    #    "debts",
    "discountCampaigns",
    "discountCodes",
    "discounts",
    "documentTypes",
    #    "driverCountryRules",
    #    "driverDebtPayouts",
    #    "driverInsuranceLogs",
    "dropoffPositions",
    #    "dynamicFormConfig",
    "groups",
    "invoices",
    "journeys",
    "landmasses",
    "languages",
    "locations",
    "meetingPositions",
    "numberingTemplates",
    #    "offers",
    "orders",
    "partners",
    "payments",
    #    "payouts",
    #    "penalties",
    "permissions",
    "poolLocations",
    "poolRoutes",
    "referralCodes",
    "regions",
    "reviews",
    "rides",
    "routes",
    #    "subsidies",
    "tags",
    "translations",
    "travelData",
    "travelVouchers",
    "unavailability",
    "userRoles",
    #    "userVerificationCodes",
    "users",
    "vehicleMakes",
    "vehicleModels",
    "vehicles",
]
EXPORT_DIR = (
    "/Users/tadeasfort/Documents/pythonJSprojects/gitLab/mongo2arango/data_mongo"
)

BATCH_SIZE = 8000


def get_collection_count(collection_name):
    cmd = ["mongo", MONGODB_URI, "--eval", f"db.{collection_name}.count()"]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise Exception(f"Error getting count for {collection_name}: {stderr.decode()}")
    else:
        count = int(stdout.decode().split("\n")[-2])
        return count


def export_collection(collection_name, pbar, count, skip=0, limit=None):
    cmd = [
        "mongoexport",
        "--uri",
        MONGODB_URI,
        "--collection",
        collection_name,
        "--jsonArray",
    ]
    if limit:
        cmd.extend(["--limit", str(limit)])
    if skip:
        cmd.extend(["--skip", str(skip)])

    batch_num = (skip // BATCH_SIZE) + 1
    if count > BATCH_SIZE:  # Check if the collection is large enough to be batched
        output_file = f"{EXPORT_DIR}/{collection_name}/batch{batch_num}.json"
    else:
        output_file = f"{EXPORT_DIR}/{collection_name}.json"

    cmd.extend(["--out", output_file])
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    pbar.update(1)
    if process.returncode != 0:
        raise Exception(f"Error exporting {collection_name}: {stderr.decode()}")


def export_collections_threaded(collections, main_pbar, threads):
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {}
        pbar = tqdm(
            total=sum([get_collection_count(col) for col in collections]),
            desc="Processing Collections",
            leave=True,
        )
        for col in collections:
            count = get_collection_count(col)
            if count > BATCH_SIZE:
                os.makedirs(f"{EXPORT_DIR}/{col}", exist_ok=True)
                for skip in range(0, count, BATCH_SIZE):
                    futures[
                        executor.submit(
                            export_collection, col, pbar, count, skip, BATCH_SIZE
                        )
                    ] = col
            else:
                futures[executor.submit(export_collection, col, pbar, count)] = col
        for future in as_completed(futures):
            col = futures[future]
            try:
                main_pbar.update(1)
            except Exception as e:
                print(f"Error processing collection {col}: {e}")
            finally:
                pbar.update(1)
        pbar.close()


@click.command()
@click.option(
    "-t",
    "--threads",
    default=os.cpu_count(),
    help="Number of threads to use for downloading.",
)
def main(threads):
    global EXPORT_DIR
    EXPORT_DIR = click.prompt(
        "Please provide the directory path where exported files will be saved",
        type=str,
        default=EXPORT_DIR,
    )
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)
    with tqdm(
        total=len(COLLECTIONS_TO_EXPORT),
        desc="Overall Progress",
        position=0,
        leave=True,
    ) as main_pbar:
        export_collections_threaded(COLLECTIONS_TO_EXPORT, main_pbar, threads)


if __name__ == "__main__":
    main()
