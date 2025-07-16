#!/usr/bin/env python3
"""
FSM State Machine Example

Demonstrates:
- FSM with multiple states
- State transitions
- Stop conditions
- Step-by-step execution
- Context tracking across cycles
"""

import argparse
from fast_dag import FSM, FSMReturn, FSMContext


def main():
    parser = argparse.ArgumentParser(description="FSM State Machine Example")
    parser.add_argument(
        "--mode", choices=["sync", "async"], default="sync", help="Execution mode"
    )
    parser.add_argument(
        "--max-cycles", type=int, default=10, help="Maximum cycles before stopping"
    )
    args = parser.parse_args()

    # Create FSM for a simple traffic light system
    fsm = FSM("traffic_light", max_cycles=args.max_cycles)

    # Define states
    @fsm.state(initial=True)
    def red(context: FSMContext) -> FSMReturn:
        """Red light state"""
        print("🔴 RED - Stop!")

        # Track how long we've been red
        red_count = context.metadata.get("red_count", 0) + 1
        context.metadata["red_count"] = red_count

        # After 3 cycles, go to green
        if red_count >= 3:
            context.metadata["red_count"] = 0
            return FSMReturn(
                next_state="green",
                value={"light": "red", "action": "stop", "duration": red_count},
            )

        # Stay red
        return FSMReturn(
            next_state="red",
            value={"light": "red", "action": "stop", "duration": red_count},
        )

    @fsm.state
    def green(context: FSMContext) -> FSMReturn:
        """Green light state"""
        print("🟢 GREEN - Go!")

        # Track green duration
        green_count = context.metadata.get("green_count", 0) + 1
        context.metadata["green_count"] = green_count

        # Check for emergency stop
        if context.metadata.get("emergency", False):
            print("⚠️  Emergency detected!")
            return FSMReturn(
                next_state="emergency_stop",
                value={"light": "green", "action": "emergency"},
            )

        # After 4 cycles, go to yellow
        if green_count >= 4:
            context.metadata["green_count"] = 0
            return FSMReturn(
                next_state="yellow",
                value={"light": "green", "action": "go", "duration": green_count},
            )

        return FSMReturn(
            next_state="green",
            value={"light": "green", "action": "go", "duration": green_count},
        )

    @fsm.state
    def yellow(context: FSMContext) -> FSMReturn:
        """Yellow light state"""
        print("🟡 YELLOW - Caution!")

        # Yellow is always brief - 1 cycle
        return FSMReturn(
            next_state="red", value={"light": "yellow", "action": "caution"}
        )

    @fsm.state(terminal=True)
    def emergency_stop(context: FSMContext) -> FSMReturn:
        """Emergency stop state"""
        print("🚨 EMERGENCY STOP!")

        # This is a terminal state
        return FSMReturn(
            # note: next_state not needed, as terminal
            value={"light": "emergency", "action": "stop", "reason": "emergency"},
            stop=True,
        )

    # Additional terminal state
    @fsm.state(terminal=True)
    def maintenance(context: FSMContext) -> FSMReturn:
        """Maintenance mode"""
        print("🔧 MAINTENANCE MODE")

        return FSMReturn(
            # note: next_state not needed, as terminal
            value={"light": "off", "action": "maintenance"},
            stop=True,
        )

    # set the initial state (if not used the initial flag above)
    # fsm.set_initial_state("red")

    # Validate FSM
    print("Validating FSM...")
    fsm.validate_or_raise()
    print("✓ FSM is valid")

    # Visualize
    print("\nGenerating state diagram...")
    fsm.save_mermaid("traffic_light_fsm.mmd")

    # Test 1: Normal operation with step-by-step execution
    print("\n" + "=" * 50)
    print("Test 1: Step-by-step execution")
    print("=" * 50)

    context = FSMContext()
    cycle = 0

    while cycle < 12:  # Run for 12 steps
        print(f"\nCycle {cycle + 1}:")

        # Execute one step
        context, result = fsm.step(context)

        # Show result
        print(f"Result: {result}")

        # Check if we should stop
        if fsm.is_terminal_state(fsm.current_state):
            print("Reached terminal state, stopping.")
            break

        cycle += 1

    print(f"\nTotal cycles executed: {context.cycle_count}")
    print(f"State history: {context.state_history}")

    # Test 2: Emergency scenario
    print("\n" + "=" * 50)
    print("Test 2: Emergency scenario")
    print("=" * 50)

    context2 = FSMContext()
    context2.metadata["emergency"] = False

    for i in range(8):
        print(f"\nCycle {i + 1}:")

        # Trigger emergency after 5 cycles
        if i == 5:
            print("*** TRIGGERING EMERGENCY ***")
            context2.metadata["emergency"] = True

        context2, result = fsm.step(context2)
        print(f"State: {fsm.current_state}, Result: {result}")

        if fsm.is_terminal_state(fsm.current_state):
            print("Emergency stop activated!")
            break

    # Test 3: Full run with max cycles
    print("\n" + "=" * 50)
    print(f"Test 3: Full run (max {args.max_cycles} cycles)")
    print("=" * 50)

    # note: make context available to the outside
    context = FSMContext()
    if args.mode == "sync":
        final_result = fsm.run(context=context)
    else:
        import asyncio

        final_result = asyncio.run(fsm.run_async(context=context))

    print(f"\nFinal result: {final_result}")

    # note: use the context - as that contains execution history
    if args.viz:
        fsm.save_mermaid("traffic_light_fsm_executed.mmd", context=context)

    # Access results by state name
    print("\nAccessing state results:")
    print(f"Latest red state: {fsm['red']}")
    print(f"Latest green state: {fsm['green']}")

    # Access specific cycles
    if len(fsm.get_history("red")) > 2:
        print(f"Red state at cycle 2: {fsm['red.2']}")


if __name__ == "__main__":
    main()
