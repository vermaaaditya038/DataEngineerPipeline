Confidence Level of Different Attributes


Confidence on Price:

Confidence in Price Format: If the price was extracted from a clean, numeric string or standard format, it could have a higher confidence level but if written number (e.g., "one hundred"), the confidence level might be lower.

Missing Price Confidence: If a price is missing, the confidence level should be low (0). If it's present but contains unusual characters, it should be flagged as uncertain.


def price_confidence(price):
    if isinstance(price, float) and price > 0:
        return 1.0  # High confidence if price is valid
    elif isinstance(price, str):
        try:
            # If price is text-based and was converted successfully
            w2n.word_to_num(price.lower())
            return 0.7  # Moderate confidence for text-based conversion
        except ValueError:
            return 0.4  # Low confidence if price is malformed
    return 0.0  # No confidence for missing or invalid prices

df['PriceConfidence'] = df['Price'].apply(price_confidence)

Confidence in Date Format:

Valid Date: If the date is valid and standardized, the confidence will be high.
Missing Date: If the date is missing or couldn't be parsed, assign a low confidence level.

def date_confidence(date):
    if pd.notnull(date):
        return 1.0  # High confidence if the date is valid
    return 0.0  # No confidence if the date is missing or invalid

df['DateConfidence'] = df['DateAdded'].apply(date_confidence)


Confidence in Quantity:

Valid Quantity: If the quantity is an integer or a valid number, it should have high confidence.
Missing Quantity: If missing, assume low confidence (but impute with zero if necessary).



def quantity_confidence(quantity):
    if isinstance(quantity, int) and quantity >= 0:
        return 1.0  # High confidence for valid quantities
    return 0.0  # No confidence for missing or invalid quantities

df['QuantityConfidence'] = df['Quantity'].apply(quantity_confidence)

