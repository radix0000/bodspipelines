from bodspipelines.infrastructure.pipeline import Source, Stage, Pipeline
from bodspipelines.infrastructure.storage import ElasticStorage
from bodspipelines.infrastructure.outputs import Output, OutputConsole, NewOutput, KinesisOutput
from bodspipelines.infrastructure.processing.bulk_data import BulkData
from bodspipelines.infrastructure.processing.xml_data import XMLData
from bodspipelines.transforms.gleif import Gleif2Bods
from bodspipelines.mappings.gleif import lei_properties, rr_propertties, repex_properties, match_lei, match_rr, match_repex

# Defintion of LEI-CDF v3.1 XML date source
lei2_source = Source(name="lei2",
                     origin=BulkData(display="LEI-CDF v3.1",
                                     url='https://leidata.gleif.org/api/v1/concatenated-files/lei2/get/30447/zip',
                                     size=41491,
                                     directory="lei-cdf"),
                     datatype=XMLData(item_tag="LEIRecord",
                                      namespace={"lei": "http://www.gleif.org/data/schema/leidata/2016"}))

# Defintion of RR-CDF v2.1 XML date source
rr_source = Source(name="rr",
                   origin=BulkData(display="RR-CDF v2.1",
                                   url='https://leidata.gleif.org/api/v1/concatenated-files/rr/get/30450/zip',
                                   size=2823,
                                   directory="rr-cdf"),
                   datatype=XMLData(item_tag="RelationshipRecord",
                                    namespace={"rr": "http://www.gleif.org/data/schema/rr/2016"}))

# Defintion of Reporting Exceptions v2.1 XML date source
repex_source = Source(name="repex",
                      origin=BulkData(display="Reporting Exceptions v2.1",
                                      url='https://leidata.gleif.org/api/v1/concatenated-files/repex/get/30453/zip',
                                      size=3954,
                                      directory="rep-ex"),
                      datatype=XMLData(item_tag="Exception",
                                       namespace={"repex": "http://www.gleif.org/data/schema/repex/2016"}))

output_console = Output(name="console", target=OutputConsole(name="gleif-injest"))

output_new = NewOutput(storage=ElasticStorage(indexes={"lei2": {"properties": lei_properties, "match": match_lei},
                                                       "rr": {"properties": rr_propertties, "match": match_rr},
                                                       "repex": {"properties": repex_properties, "match": match_repex}}), 
                       output=output_console) # KinesisOutput(stream_name="gleif_injest_stream"))

# Definition of GLEIF data pipeline injest stage
injest_stage = Stage(name="ingest",
              sources=[rr_source], #[lei2_source, rr_source, repex_source],
              processors=[],
              outputs=[output_console]
)

# Definition of GLEIF data pipeline transform stage
#transform_stage = Stage(name="transform",
#              sources=[lei2_stream, rr_stream, repex_stream],
#              processors=[Gleif2Bods()],
#              outputs=[output_console]
#)

# Definition of GLEIF data pipeline
pipeline = Pipeline(name="gleif", stages=[injest_stage]) #, transform_stage])
