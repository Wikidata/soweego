#!/usr/bin/python

import rdflib 
import business.utils.file_utils as file_utils
import domain.localizations as loc

def bibsys_handler(dump):
    file_path = '{0}/{1}'.format(loc.temporary_directory, dump.file_name)
    if not file_utils.exists(file_path):
        raise Exception("file: {0} not found".format(file_path))
    with open(file_path) as file:
        rows = file.readlines() 
        
        current_item = []
        current_uri = None 
        is_a_person = False

        for row in rows:
            try :
                rdfxml = row.strip() 
                if rdfxml: 
                    g = rdflib.Graph() 
                    g.parse(data=rdfxml, format='nt') 

                    for s, p, o in g: 
                        if current_uri is None or current_uri == s:
                            current_uri = s
                            current_item.append(g)
                            if(''.join(p) == dump.rdf_type and ''.join(o) == dump.rdf_person):
                                is_a_person = True
                        else :
                            for prop in current_item:    
                                for s, p, o in prop:    
                                    print(s + '\t' + p + '\t' + o)  
                            is_a_person = False
                            current_item = []
                            current_uri = s
            except : 
                file_utils.log_error("Error at row: {0}".format(row))
        #file_utils.log(loc.output_file, file_utils.output)

def bibsys_schema(dump):
    file_path = '{0}/{1}'.format(loc.temporary_directory, dump.file_name)
    if not file_utils.exists(file_path):
        raise Exception("file: {0} not found".format(file_path))
    with open(file_path) as file:
        rows = file.readlines() 
        schema = set()
        for row in rows:
            try :
                rdfxml = row.strip() 
                if rdfxml: 
                    g = rdflib.Graph() 
                    g.parse(data=rdfxml, format='nt') 
                    for s, p, o in g: 
                        schema.add(p)
            except : 
                file_utils.log_error("Error at row: {0}".format(row))
        
        for prop in schema:
            file_utils.log(''.join(prop) + '\n', '{0}/{1}_schema.json'.format(loc.assets_directory, dump.name))