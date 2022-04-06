import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from src.sources.source_fromflatarray import SourceFromFlatArray
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestSource(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.l_data = [1, 4, 5, 6]
        self.source = SourceFromFlatArray(conf={'file_path': 'tests/asset/sg_t16_mini.npy'})
        self.nb_elements = 3

    def test_generator(self):
        # Test the generator - classic
        gen = self.source.generator()
        out = []
        for _ in range(self.nb_elements):
            try:
                m = next(gen)
                out.append(m.get('data'))
            except StopIteration:
                logger.debug('Stop generator')
                break

        self.assertEqual(len(out), self.nb_elements)

    # def test_process(self):
    #     # Test process
    #     out = []
    #     message = self.mock_source.process()

    #     while END_SOURCE_PIPELINE not in message:
    #         out.append(message.get('data'))
    #         message = self.mock_source.process()

    #     self.assertListEqual(out, self.l_data)
