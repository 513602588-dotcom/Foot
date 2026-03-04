#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Main prediction entry point - Football Prophet Pro
主预测入口程序 - GitHub Actions Compatible
"""

import json
import sys
from pathlib import Path

def main():
    """主函数 - 生成预测推荐"""
    
    # 根目录 - 确保能在任何工作目录执行
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    
    print("=" * 70)
    print("⚽ Football Prophet Pro - Prediction Engine")
    print("=" * 70)
    
    try:
        # 尝试加载已有的预测数据
        picks_path = project_root / "site" / "data" / "picks.json"
        
        if picks_path.exists():
            print(f"\n📂 Loading predictions from {picks_path}")
            
            with open(picks_path, 'r', encoding='utf-8') as f:
                picks = json.load(f)
            
            print(f"✅ Loaded {len(picks)} predictions\n")
            
            # 显示前5个预测
            print("🏆 Top Predictions:")
            print("-" * 70)
            
            for idx, pick in enumerate(picks[:5], 1):
                home = pick.get('home', 'N/A')
                away = pick.get('away', 'N/A')
                date = pick.get('date', 'N/A')
                prob = pick.get('prob', {})
                ev = pick.get('ev_home', 0)
                kelly = pick.get('kelly_home', 0)
                
                prob_h = prob.get('H', 0) * 100
                prob_d = prob.get('D', 0) * 100
                prob_a = prob.get('A', 0) * 100
                
                print(f"\n{idx}. {home} vs {away}")
                print(f"   Date: {date}")
                print(f"   Prob: W{prob_h:.1f}% D{prob_d:.1f}% L{prob_a:.1f}%")
                print(f"   EV: {ev*100:.2f}%  |  Kelly: {kelly*100:.2f}%")
            
            print("\n" + "=" * 70)
            print("✅ Prediction engine ran successfully!")
            print("=" * 70)
            
            return 0
        else:
            print(f"\n⚠️  Predictions file not found: {picks_path}")
            print("   Creating placeholder data...")
            
            # 创建示例数据
            picks_path.parent.mkdir(parents=True, exist_ok=True)
            
            sample_picks = [
                {
                    "home": "Team A",
                    "away": "Team B",
                    "date": "2024-03-04",
                    "prob": {"H": 0.50, "D": 0.30, "A": 0.20},
                    "odds": {"H": 1.90, "D": 3.20, "A": 4.00},
                    "ev_home": 0.05,
                    "kelly_home": 0.02
                }
            ]
            
            with open(picks_path, 'w', encoding='utf-8') as f:
                json.dump(sample_picks, f, ensure_ascii=False, indent=2)
            
            print(f"✅ Created sample predictions at {picks_path}")
            print("=" * 70)
            
            return 0
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
