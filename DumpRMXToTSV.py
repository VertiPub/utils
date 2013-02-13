import codecs
import xml.parsers.expat
import sys, traceback

# 
# Soam Acharya - (c) 2013, VertiCloud Inc

class DumpRMXtoTSV:

    Parser = ""

    # prepare for parsing

    def __init__(self, xml_file):
        assert(xml_file != "")
        self.xml_file = xml_file

        self.Parser = xml.parsers.expat.ParserCreate('utf-8')
        self.Parser.CharacterDataHandler = self.handleCharData
        self.Parser.StartElementHandler = self.handleStartElement
        self.Parser.EndElementHandler = self.handleEndElement

        self.in_header = 0
        self.in_row = 0
        self.in_column = 0

        
    # parse the XML file
        
    def parse(self):
        try:
            # have to deal with non ASCII characters in input XML - it's UTF8 encoded
            input_file = codecs.open(self.xml_file, encoding='utf-8')
            for line in input_file:
                # self.Parser.Parse(line)
                self.Parser.Parse(line.encode('utf-8'))
            self.Parser.Parse("", 1)
            
        except:
            sys.stderr.write ("ERROR: Can't open XML file " + self.xml_file + "!\n")
            traceback.print_exc(file=sys.stderr)
            sys.exit(0)
    

    # handlers that do the actual parsing
    
    # beginning of XML record
    def handleStartElement(self, name, attrs):
        # print name
        if name == 'HEADER':
            self.in_header = 1
        elif name == 'ROW':
            self.in_row = 1
        elif name == 'COLUMN':
            self.in_column = 1
            
    # actual contents of XML record
    def handleCharData(self, data):
        if (self.in_header == 1 and self.in_column == 1) or (self.in_row == 1 and self.in_column == 1):
            sys.stdout.write(data.encode('utf-8'))
            sys.stdout.write("\t");

        
    def handleEndElement(self, name):
        if name == 'HEADER':
            self.in_header = 0
            sys.stdout.write("\n");
        elif name == 'ROW':
            self.in_row = 0
            sys.stdout.write("\n");
        elif name == 'COLUMN':
            self.in_column = 0
    

if len(sys.argv) != 2:
    print "Expecting name of file"
    sys.exit(-1)

p = DumpRMXtoTSV(sys.argv[1]);
p.parse()

