import aws_cdk as core
import aws_cdk.assertions as assertions

from solution.solution_stack import SolutionStack

# example tests. To run these tests, uncomment this file along with the example
# resource in solution/solution_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = SolutionStack(app, "solution")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
