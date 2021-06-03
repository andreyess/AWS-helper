class AwsServiceClientNotSupported(Exception):
    """Raised when boto3 aws service name is not supported by AwsHelper"""

    def __init__(self, client_name, message="Client name is not supported by AwsHelper"):
        self.client_name = client_name
        self.message = message
        super().__init__(self.message)

class OperationWasNotConfirmed(Exception):
    """Raised when user didn't confirmed an operation"""
    
    def __init__(self,  message="An operation was not confirmed by user. Stopping the program"):
        self.message = message
        super().__init__(self.message)

class DeleteCyclesLimitReached(Exception):
    """Raised when bucket delete objects cycles count was reached"""
    
    def __init__(self, cyclesCount: int,  message="Delete objects cycles count in your bucket was reached: {}. Stopping the program"):
        self.cyclesCount = cyclesCount
        self.message = message.format(cyclesCount)
        super().__init__(self.message)
