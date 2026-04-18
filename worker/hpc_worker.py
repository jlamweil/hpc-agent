#!/usr/bin/env python3
"""HPC worker using Hermes skills for multi-pass processing."""
import os
import re
import sys
import json
import time
import sqlite3
import argparse

HERO_DIR = os.path.dirname(os.path.abspath(__file__))
ASYNC_HERMES = os.path.join(os.path.dirname(HERO_DIR), "async-hermes-agent")
sys.path.insert(0, ASYNC_HERMES)

from transformers import AutoTokenizer, AutoModelForCausalLM
import torch


class SkillLoader:
    """Load Hermes skills from ~/.hermes/skills/"""
    
    SKILLS_DIR = os.path.expanduser("~/.hermes/skills")
    
    @classmethod
    def load_skill(cls, skill_name: str) -> str:
        """Load skill content from SKILL.md"""
        skill_path = os.path.join(cls.SKILLS_DIR, skill_name, "SKILL.md")
        if not os.path.exists(skill_path):
            raise ValueError(f"Skill not found: {skill_name}")
        
        with open(skill_path) as f:
            content = f.read()
        
        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) >= 3:
                content = parts[2]
        
        return content.strip()
    
    @classmethod
    def format_skill(cls, skill_name: str, **kwargs) -> str:
        """Format skill template with variables"""
        content = cls.load_skill(skill_name)
        return content.format(**kwargs)


class HPCWorker:
    """HPC worker using Hermes skills for multi-pass processing."""
    
    def __init__(self, model_name: str = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"):
        self.model_name = model_name
        self.tokenizer = None
        self.model = None
    
    def _load_model(self):
        if self.model is None:
            print(f"Loading {self.model_name}...")
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                torch_dtype=torch.float16,
                device_map="auto"
            )
            print("Model loaded!")
    
    def generate(self, prompt: str, max_new_tokens: int = 256) -> str:
        self._load_model()
        
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.model.device)
        outputs = self.model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            do_sample=True,
            temperature=0.7,
        )
        response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        if response.startswith(prompt):
            response = response[len(prompt):].strip()
        
        return response
    
    def extract_code(self, text: str) -> str:
        match = re.search(r"```python\n(.*?)```", text, re.DOTALL)
        if match:
            return match.group(1).strip()
        if "```" in text:
            lines = text.split("```")
            if len(lines) >= 3:
                return lines[1].strip()
        return text.strip().split("\n")[0]
    
    def process_with_skills(self, task_prompt: str, 
                           use_reviewer: bool = True,
                           max_revisions: int = 1) -> dict:
        coder_prompt = SkillLoader.format_skill("coder", prompt=task_prompt)
        code_result = self.generate(coder_prompt)
        code = self.extract_code(code_result)
        
        print(f"Generated code: {code[:100]}...")
        
        if not use_reviewer:
            return {"code": code, "status": "completed", "revisions": 0}
        
        reviewer_prompt = SkillLoader.format_skill("reviewer", code=code)
        review_result = self.generate(reviewer_prompt)
        
        print(f"Review: {review_result[:200]}...")
        
        revisions = 0
        final_code = code
        
        if "needs_work" in review_result.lower() or "issues found" in review_result.lower():
            for i in range(max_revisions):
                revise_prompt = f"""Based on this review:

{review_result}

Original task: {task_prompt}

Please generate revised code:
```python
"""
                revised = self.generate(revise_prompt, max_new_tokens=512)
                final_code = self.extract_code(revised)
                revisions += 1
                print(f"Revision {revisions}: {final_code[:100]}...")
        
        return {
            "code": final_code,
            "review": review_result,
            "status": "completed",
            "revisions": revisions
        }
    
    def process_from_db(self, db_path: str, limit: int = 10) -> int:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        tasks = conn.execute("""
            SELECT id, messages FROM tasks 
            WHERE status = 'pending' 
            ORDER BY priority DESC, created_at ASC
            LIMIT ?
        """, (limit,)).fetchall()
        
        if not tasks:
            print("No pending tasks found!")
            conn.close()
            return 0
        
        print(f"Processing {len(tasks)} tasks...")
        
        completed = 0
        for task in tasks:
            task_id = task["id"]
            messages = json.loads(task["messages"])
            prompt = messages[0]["content"]
            
            try:
                print(f"\n--- Task {task_id[:8]} ---")
                print(f"Prompt: {prompt[:80]}...")
                
                result = self.process_with_skills(prompt)
                
                conn.execute("""
                    UPDATE tasks SET status = 'completed', completed_at = ?, result = ?
                    WHERE id = ?
                """, (time.time(), json.dumps(result), task_id))
                conn.commit()
                
                completed += 1
                print(f"Completed: {result['status']}, revisions: {result.get('revisions', 0)}")
                
            except Exception as e:
                print(f"Failed: {e}")
                conn.execute("""
                    UPDATE tasks SET status = 'failed', error = ?
                    WHERE id = ?
                """, (str(e), task_id))
                conn.commit()
        
        conn.close()
        return completed


def main():
    parser = argparse.ArgumentParser(description="HPC worker with Hermes skills")
    parser.add_argument("--db", default="tasks.db", help="SQLite database path")
    parser.add_argument("--model", default="TinyLlama/TinyLlama-1.1B-Chat-v1.0")
    parser.add_argument("--limit", type=int, default=10, help="Max tasks to process")
    parser.add_argument("--reviewer", action="store_true", help="Enable reviewer skill")
    parser.add_argument("--revisions", type=int, default=1, help="Max revisions")
    parser.add_argument("--skill", help="Test single skill with prompt")
    args = parser.parse_args()
    
    worker = HPCWorker(model_name=args.model)
    
    if args.skill:
        prompt = SkillLoader.format_skill(args.skill, prompt=args.skill)
        result = worker.generate(prompt)
        print(f"\nSkill: {args.skill}")
        print(f"Result: {result[:500]}...")
    else:
        count = worker.process_from_db(args.db, args.limit)
        print(f"\nProcessed {count} tasks")


if __name__ == "__main__":
    main()