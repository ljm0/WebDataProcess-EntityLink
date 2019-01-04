### html2text.py

from bs4 import BeautifulSoup, Comment
# import re

def record2html(record):
### find html in warc file
    ishtml = False;
    html = "";
    for line in record.splitlines():
        ### html starts with <html
        if line.startswith("<html"):
            ishtml = True;
        if ishtml:
            html += line;
    return html;

#get the text
def html2text(record):
    html_doc = record2html(record);
    # Rule = "/<.*>/";

    if html_doc:
        soup = BeautifulSoup(html_doc,"html.parser");
        ### get title in <p></p>
        title = soup.find("title").get_text(strip = True);
        ### remove tags: <script> <style> <code> <title> <head>
        [s.extract() for s in soup(['script','style', 'code','title','head'])]
        ### remove tags id=["footer", "header"]
        [s.extract() for s in soup.find_all(id = ['footer', 'header'])]
        ### remove comments
        for element in soup(s=lambda s: isinstance(s, Comment)):
            element.extract()
        # text = soup.get_text("\n", strip=True);

        ### get text in <p></p>
        paragraph = soup.find_all("p");
        text = ""
        for p in paragraph:
            if p.get_text(" ", strip=True) != '':
                text += p.get_text(" ", strip=True)+"\n"

        # text = re.sub(Rule, "", text);
        # escape character
        # soup_sec = BeautifulSoup(text,"html.parser");

        return text, title;
    return "","";
