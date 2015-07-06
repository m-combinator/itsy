(ns itsy.extract
  (:import (java.io ByteArrayInputStream)
           (org.apache.tika.sax BodyContentHandler)
           (org.apache.tika.metadata Metadata)
           (org.apache.tika.parser ParseContext)
           (org.apache.tika.parser.html HtmlParser)
           (org.apache.tika.sax LinkContentHandler)
           (java.nio.charset StandardCharsets)))

(defn html->str
  "Convert HTML to plain text using Apache Tika"
  [body]
  (when body
    (let [bais (ByteArrayInputStream. (.getBytes body))
          handler (BodyContentHandler.)
          metadata (Metadata.)
          parser (HtmlParser.)]
      (.parse parser bais handler metadata (ParseContext.))
      (.toString handler))))

(defn extract-urls
  "extract all links from webpage. returns a seq of urls"
  [body]
  (when body
    (let [link-handler (new LinkContentHandler)
          html-parser (new HtmlParser)
          parse-context (new ParseContext)
          metadata (new Metadata)
          inputstream (new ByteArrayInputStream (. body
                                                   getBytes
                                                   (StandardCharsets/UTF_8)))]
      (. html-parser parse inputstream link-handler metadata parse-context)
      (map #(. % getUri) (. link-handler getLinks)))))
