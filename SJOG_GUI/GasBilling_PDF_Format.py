import fitz
import pandas as pd
import re


def gas_billing(pdf_path, page_number=3):
    def get_text_by_position(blocks, target_top, target_left=None, tol_top=1.0, tol_left=5.0, multiline=False):
        for block in blocks:
            x0, y0, _, _, text, *_ = block
            if abs(y0 - target_top) <= tol_top:
                if target_left is None or abs(x0 - target_left) <= tol_left:
                    return text.strip() if not multiline else text
        return None

    with fitz.open(pdf_path) as doc:
        page = doc[page_number]
        blocks = page.get_text("blocks")
        blocks = [b for b in blocks if b[1] < 400]

        for block in blocks:
            x0, y0, x1, y1, text, *_ = block
            # print(f"top={y0:.1f}, left={x0:.1f}, text={text.strip()}")
        address = get_text_by_position(blocks, 32.4, 28.5)
        address_match = re.search(r"(\d+.*?)(MIDLAND WA)", address)
        energy = get_text_by_position(blocks, 194.8, 29.5).splitlines()[0]
        usage = get_text_by_position(blocks, 250.5, 29.5).splitlines()[0]
        usage_match = float(re.sub(r"[^\d.]", "", usage))
        stand = get_text_by_position(blocks, 237.7, 29.5).splitlines()[0]
        stand_match = float(re.sub(r"[^\d.]", "", stand))
        demand = get_text_by_position(blocks, 225.0, 29.5).splitlines()[0]
        demand_match = float(re.sub(r"[^\d.]", "", demand))
        total = get_text_by_position(blocks, 316.2, 29.5).splitlines()[1]

        info = {
            "ACCTNO": 605628,
            "ACCOUNT": 'SJG Midland Hospital',
            "SITE ADDRESS": f"{address_match.group(1).strip()}, {address_match.group(2)}",
            "INVOICE #": get_text_by_position(blocks, 55.4, 317.0, multiline=True).splitlines()[0],
            "NMI": get_text_by_position(blocks, 55.4, 317.0, multiline=True).splitlines()[2],
            "BILLING PERIOD START DATE": pd.to_datetime(get_text_by_position(blocks, 122.4, 283.6).splitlines()[0],
                                                        errors="coerce"),
            "BILLING PERIOD END DATE": pd.to_datetime(get_text_by_position(blocks, 122.4, 283.6).splitlines()[1],
                                                      errors="coerce"),
            "BILLING PERIOD NUMBER OF DAYS": int(
                re.sub(r"[^\d]", "", get_text_by_position(blocks, 122.4, 283.6).splitlines()[2])
            ),
            "TOTAL GAS CONSUMPTION (GJ)": float(
                re.sub(r"[^\d.]", "", get_text_by_position(blocks, 194.8, 29.5).splitlines()[3])
            ),
            "TOTAL ENERGY SPEND $": float(re.sub(r"[^\d.]", "", energy)),
            "NETWORK CHARGE ($)": usage_match + stand_match + demand_match,
            "DAILY SUPPLY CHARGE ($)": 0,
            "OTHER CHARGES": 0,
            "INVOICE TOTAL ($)": float(re.sub(r"[^\d.]", "", total))
        }

        return pd.DataFrame([info])

