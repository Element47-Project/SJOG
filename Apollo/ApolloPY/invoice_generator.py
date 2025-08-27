import pandas as pd
import logging


def generate_invoice(consumption, tariff, date, meter):
    try:
        invoice_rows = []
        for cons in (consumption if isinstance(consumption, list) else [consumption]):
            invoice_row = {
                "Date": date,
                "Meter": meter,
                "EXP": cons.get("EXP"),
                "Consumption": sum([value for key, value in cons.items() if key != "EXP" and value is not None]),
                "Fixed_Daily": tariff.get("Fixed_Price", 0),
                "Anytime": (cons.get("Anytime") or 0) * tariff.get("Anytime_Rate", 0),
                "On_Peak": (cons.get("Peak") or 0) * tariff.get("On_Peak_Rate", 0),
                "Off_Peak": (cons.get("Off-Peak") or 0) * tariff.get("Off_Peak_Rate", 0),
                "Shoulder": (cons.get("Shoulder") or 0) * tariff.get("Shoulder_Rate", 0),
                "Overnight": (cons.get("Overnight") or 0) * tariff.get("Overnight_Rate", 0),
                "Super_Off_Peak": (cons.get("Super_Off_Peak") or 0) * tariff.get("Super_Off_Peak_Rate", 0),
                "Total": sum([
                    tariff.get("Fixed_Price", 0),
                    (cons.get("Anytime") or 0) * tariff.get("Anytime_Rate", 0),
                    (cons.get("Peak") or 0) * tariff.get("On_Peak_Rate", 0),
                    (cons.get("Off-Peak") or 0) * tariff.get("Off_Peak_Rate", 0),
                    (cons.get("Shoulder") or 0) * tariff.get("Shoulder_Rate", 0),
                    (cons.get("Overnight") or 0) * tariff.get("Overnight_Rate", 0),
                    (cons.get("Super_Off_Peak") or 0) * tariff.get("Super_Off_Peak_Rate", 0)
                ])
            }
            invoice_rows.append(invoice_row)
        return pd.DataFrame(invoice_rows)
    except Exception as e:
        logging.error(f"Error generating invoice: {e}")
        return None
