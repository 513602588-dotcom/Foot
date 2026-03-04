#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
src module entry point - supports 'python -m src.build' style execution
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if __name__ == "__main__":
    """When called directly, delegate to build.py"""
    from src.build import main
    main()
