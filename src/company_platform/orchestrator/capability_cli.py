from company_platform.orchestrator.capabilities import (
    COMPONENT_CAPABILITIES,
)


def main():
    print("COMPONENT CAPABILITIES")
    print("=" * 70)

    for component_id, capability in COMPONENT_CAPABILITIES.items():
        print(
            f"{component_id:28} "
            f"{capability.state:18} "
            f"{capability.reason}"
        )


if __name__ == "__main__":
    main()