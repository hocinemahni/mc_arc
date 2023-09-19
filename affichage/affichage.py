class view:
  def __repr__(self) -> str:
        output = "\n".join(("",
            f't1|empty|t2 (▓.▒) [{"▓"*int(len(self.t1)*50/self.c)}' \
            f'{"."*int(50-(len(self.t1)+len(self.t2))*50/self.c)}' \
            f'{"▒"*int(len(self.t2)*50/self.c)}] (len(t1)={len(self.t1)}'\
            f', len(t2)={len(self.t2)}, total={self.c}, unused={self.c-len(self.t1)-len(self.t2)})',"",
            f'b1|empty|b2 (▓.▒) [{"▓"*int(len(self.b1)*50/self.c)}' \
            f'{"."*int(50-(len(self.b1)+len(self.b2))*50/self.c)}' \
            f'{"▒"*int(len(self.b2)*50/self.c)}] (len(b1)={len(self.b1)}'\
            f', len(b2)={len(self.b2)}, total={self.c}, unused={self.c-len(self.b1)-len(self.b2)})'))
        t1_content = [f'{file.name}: [{", ".join(sorted([str(block_offset2) for file2, block_offset2 in self.t1.keys() if file2 == file]))}]'
                      for file in set([file for file, block_offset in self.t1.keys()])]
        t2_content = [f'{file.name}: [{", ".join(sorted([str(block_offset2) for file2, block_offset2 in self.t2.keys() if file2 == file]))}]'
                      for file in set([file for file, block_offset in self.t2.keys()])]
        b1_content = [f'{file.name}: [{", ".join(sorted([str(block_offset2) for file2, block_offset2 in self.b1.keys() if file2 == file]))}]'
                      for file in set([file for file, block_offset in self.b1.keys()])]
        b2_content = [f'{file.name}: [{", ".join(sorted([str(block_offset2) for file2, block_offset2 in self.b2.keys() if file2 == file]))}]'
                      for file in set([file for file, block_offset in self.b2.keys()])]
        output += "\n".join((["\n\nt1 is empty", "\n\nt1 contains:\n  - "+"\n  - ".join(t1_content)][len(self.t1)>0],
                           ["t2 is empty", "t2 contains:\n  - "+"\n  - ".join(t2_content)][len(self.t2)>0],
                           ["b1 is empty", "b1 contains:\n  - "+"\n  - ".join(b1_content)][len(self.b1)>0],
                           ["b2 is empty", "b2 contains:\n  - "+"\n  - ".join(b2_content)][len(self.b2)>0], "#"*65))
        return output
