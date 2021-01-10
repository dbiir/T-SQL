# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import ytest_framework_msg_pb2 as ytest__framework__msg__pb2


class YTestStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.GetRequest = channel.unary_unary(
        '/YTest/GetRequest',
        request_serializer=ytest__framework__msg__pb2.YTestRequest.SerializeToString,
        response_deserializer=ytest__framework__msg__pb2.YTestResponse.FromString,
        )


class YTestServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def GetRequest(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_YTestServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'GetRequest': grpc.unary_unary_rpc_method_handler(
          servicer.GetRequest,
          request_deserializer=ytest__framework__msg__pb2.YTestRequest.FromString,
          response_serializer=ytest__framework__msg__pb2.YTestResponse.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'YTest', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))