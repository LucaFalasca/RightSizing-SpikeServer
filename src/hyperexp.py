from rvgs import Uniform, Exponential

def Hyperexponential(mean,cv):
  #=========================================================
  #Returns a hyperexponentially distributed positive real number. 
  #NOTE: use mean > 0.0 and cv > 1.0
  #=========================================================
  #
  c = (cv * cv - 1.0) / (cv * cv + 1.0)
  p1 = 0.5 * (1.0 + c)
  p2 = 1.0 - p1
  m1 = mean / (2.0 * p1)
  m2 = mean / (2.0 * p2)

  r = Uniform(0.0,1.0)

  if (r < p1):
    return (Exponential(m1))
  else:
    return (Exponential(m2))