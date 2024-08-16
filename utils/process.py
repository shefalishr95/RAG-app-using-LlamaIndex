import re
from html import unescape

# Define patterns to remove specific content
image_pattern = re.compile(r"!\[.*\]\(.*\)")
link_pattern = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
html_tag_pattern = re.compile(r"<.*?>")
whitespace_pattern = re.compile(r"\s+")
emoji_pattern = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F1E0-\U0001F1FF"  # flags (iOS)
    "\U00002500-\U00002BEF"  # chinese char
    "\U00002702-\U000027B0"  # dingbats
    "\U00002702-\U000027B0"  # dingbats (duplicate, may be a mistake)
    "\U000024C2-\U0001F251"  # enclosed characters
    "\U0001f926-\U0001f937"  # supplemental symbols and pictographs
    "\U00010000-\U0010ffff"  # supplementary private use area-A
    "\u2640-\u2642"  # gender symbols
    "\u2600-\u2B55"  # miscellaneous symbols
    "\u200d"  # zero-width joiner
    "\u23cf"  # eject symbol
    "\u23e9"  # black right-pointing double triangle
    "\u231a"  # watch
    "\ufe0f"  # variation selector-16
    "\u3030"  # wavy dash
    "]+",
    flags=re.UNICODE,
)

# Additional patterns
special_char_pattern = re.compile(r"[^\w\s\|.,;:!?-]")
multiple_space_pattern = re.compile(r"\s+")
multiple_newline_pattern = re.compile(r"\n+")

# stop_words = set(stopwords.words("english"))


def preprocess_data(doc):
    """Function to remove unwanted content from README.md"""
    doc = unescape(doc)  # Convert HTML entities to plain text
    doc = re.sub(image_pattern, "", doc)  # Remove badges and images
    doc = re.sub(link_pattern, r"\1", doc)  # Remove markdown links
    doc = re.sub(html_tag_pattern, "", doc)  # Remove HTML tags
    doc = re.sub(emoji_pattern, "", doc)  # Remove emojis and special characters
    doc = re.sub(
        special_char_pattern, "", doc
    )  # Remove other special characters but keep markdown tables
    doc = re.sub(whitespace_pattern, " ", doc).strip()  # Remove extra whitespace

    doc = doc.lower()  # Convert to lowercase

    # Consolidate multiple spaces and newlines
    doc = doc.replace("|", "\n")
    # doc = re.sub(
    #     multiple_space_pattern, " ", doc
    # )  # Replace multiple spaces with a single space
    doc = re.sub(
        multiple_newline_pattern, "\n", doc
    )  # Replace multiple newlines with a single newline

    return doc
