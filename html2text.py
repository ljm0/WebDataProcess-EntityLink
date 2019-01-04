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
    useless_tags = ['footer', 'header', 'sidebar', 'sidebar-right', 'sidebar-left', 'sidebar-wrapper', 'wrapwidget', 'widget']
    if html_doc:
        soup = BeautifulSoup(html_doc,"html.parser");
        ### remove tags: <script> <style> <code> <title> <head>
        [s.extract() for s in soup(['script','style', 'code','title','head','footer','header'])]
        ### remove tags id= useless_tags
        [s.extract() for s in soup.find_all(id = useless_tags)]
        ### remove tags class = useless_tags
        [s.extract() for s in soup.find_all(name='div',attrs={"class": useless_tags})]
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
        if text ==  "":
            text = soup.get_text(" ", strip=True);
        # text = re.sub(Rule, "", text);
        # escape character
        # soup_sec = BeautifulSoup(text,"html.parser");

        return text;
    return "";
