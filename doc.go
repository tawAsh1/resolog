// Package resolog is a resource-aware CloudWatch log tailer.
//
// Most tools answer "how do I tail a log group?" (lucagrulla/cw, aws logs
// tail, StartLiveTail). resolog answers the question that comes before that:
// "given a resource, what should I even tail?" — and then interleaves every
// stream it discovers, docker-compose style.
//
// The design has three orthogonal seams. A consumer can plug in at any of
// them; nothing forces data through the whole stack.
//
//	Resolver  resource reference -> log sources (+ a terminal signal)
//	Backend   a log source       -> a stream of events
//	Sink      a stream of events -> output (the default Sink is a TUI renderer)
//
// The flagship Resolver is sfn-execution: hand it a Step Functions execution
// ARN and it resolves the state machine plus every Lambda / Batch / ECS task
// it ran, and tails them all together.
//
// This package (the module root) is the core: the interface contracts and the
// Tail orchestrator that wires a Resolution through a Backend into a Sink.
// Resolvers and Backends live in subpackages and are composed explicitly by
// the consumer — there is no global registry and no plugin system. See
// cmd/resolog for the reference wiring, including the scheme dispatch table.
package resolog
