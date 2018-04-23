import pytest
import time

from . import analyze

# this allows using the fixture in all tests in this module
pytestmark = pytest.mark.usefixtures("spark_context", "streaming_context")


# Can also use a decorator such as this to use specific fixtures in specific functions
# @pytest.mark.usefixtures("spark_context", "hive_context")


def make_dstream_helper(sc, ssc, test_input):
    """ make dstream from input
    Args:
        test_input: list of lists of input rdd data
    Returns: a dstream
    """
    input_rdds = [sc.parallelize(d, 1) for d in test_input]
    input_stream = ssc.queueStream(input_rdds)
    return input_stream


def collect_helper(ssc, dstream, expected_length, block=True):
    
    result = []

    def get_output(_, rdd):
        if rdd and len(result) < expected_length:
            r = rdd.collect()
            if r:
                result.append(r)

    dstream.foreachRDD(get_output)

    if not block:
        return result

    ssc.start()

    timeout = 2
    start_time = time.time()
    while len(result) < expected_length and time.time() - start_time < timeout:
        time.sleep(0.01)
    if len(result) < expected_length:
        print("timeout after", timeout)

    return result


def test_analyze(spark_context, streaming_context):
    """ test that a single event is parsed correctly
    Args:
        spark_context: test fixture SparkContext
        streaming__context: test fixture SparkStreamingContext
    """

    test_input = [
            '{"agent":"\"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\"","clientip":"200.4.91.190","timestamp":1432595475000,"os":"compatible; MSIE 6.0; Windows NT 5.1; SV1"}',
            '{"agent":"\"Mozilla/5.0 (Windows; U; Windows NT 5.0; ja-JP; rv:1.7.12) Gecko/20050919 Firefox/1.0.7\"","clientip":"209.112.47.220","timestamp":1432595475000,"os":"Windows; U; Windows NT 5.0; ja-JP; rv:1.7.12"}',
            ' {"agent":"\"Mozilla/5.0 (Windows; U; Windows NT 5.0; ja-JP; rv:1.7.12) Gecko/20050919 Firefox/1.0.7\"","clientip":"155.157.99.22","timestamp":1432595475000,"os":"Windows; U; Windows NT 5.0; ja-JP; rv:1.7.12"},
			'{"agent":"\"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 8.51\"","clientip":"200.4.91.190","timestamp":1432595475000,"os":"compatible; MSIE 6.0; Windows NT 5.1; en"}'
        ]

    

    input_stream = make_dstream_helper(spark_context, streaming_context, test_input)

    tally = analyze.analyze_ip_count(input_stream)
    results = collect_helper(streaming_context, tally, 2)
    
    expected_results = [
        [('200.4.91.190', 2), ('209.112.47.220', 1), ('155.157.99.22', 1)]
        
    ]


    assert results == expected_results
