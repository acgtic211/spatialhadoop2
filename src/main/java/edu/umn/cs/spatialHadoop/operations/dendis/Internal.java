package edu.umn.cs.spatialHadoop.operations.dendis;

import java.util.Random;

public class Internal {
    int[] disable;
    int[] Nearest_proto;
    int[] Nearest_between;
    int[] Pre_individu;
    int[] Liste_selectionne;
    int[] free;
    double Max_distance;
    double[][] D_cluster;
    double[] Nearest;
    int[] Nearest_first;
    double[] Pre_distance;
    //int[] Order_selection;
    int[] Weight;
    int WorseWinner;
    double[] d1nn_proto;
    double Ave_d1nn_proto;
    double[] C_proto;
    double[] C_Oldproto;
    int C_stable;
    //Tri Tr;
    int Nb_isole, Nb_bruit;
    int Ref_OK;
    double Ref_distance;
    int Mass_Weight_Criterion;
    int Mass_OptionFinal;

    Random rand = new Random();

  public int InitMemory(ParamFCNNrule p)
  {
    this.disable = new int[p.MaxCluster+1];
    this.free = new int[Math.max(p.n,p.MaxCluster+1)];
    this.Nearest = new double[Math.max(p.n,p.MaxCluster+1)];
    this.Pre_individu = new int[Math.max(p.n,p.MaxCluster+1)];
    this.Pre_distance = new double[Math.max(p.n,p.MaxCluster+1)];
    this.Liste_selectionne = new int[Math.max(p.n,p.MaxCluster+1)];
    this.Nearest_proto =  new int[Math.max(p.n,p.MaxCluster+1)];
    this.Weight = new int[Math.max(p.n,p.MaxCluster+1)];
    this.d1nn_proto = new double[p.MaxCluster+1];
    //this.Order_selection = new int[Math.max(p.n,p.MaxCluster+1)];
    this.D_cluster = new double[p.MaxCluster+1][p.MaxCluster+1];//Utils.Reservmemdouble(p.MaxCluster+1,p.MaxCluster+1);
    this.Nearest_between = new int[p.MaxCluster+1];
    this.Nearest_first = new int[p.MaxCluster+1];
    this.C_proto = new double[p.p];
    this.C_Oldproto = new double[p.p];
    this.Mass_OptionFinal = p.Mass_OptionFinal;
    this.Mass_Weight_Criterion = p.Mass_WeightCriterion;
    //this.Tr = new Tri(p.MaxCluster+1);

    for (int i=0;i<p.MaxCluster+1;i++)
      this.Weight[i] = 0;

    return 1;
  }

  int InitProgramme(double [][]data, ParamFCNNrule P)
  {
    int First_instance=0;

    this.InitMemory(P);
    this.Max_distance = 1;
    First_instance = this.InitParametres(data,P);
    this.Ref_distance = -1;
    return First_instance;
  }

  private int InitParametres(double[][] data, ParamFCNNrule p) {
    int i,j,winner;
    double[] vector = new double[1000];
    double d_min = 100000;
    double dist;

    for (i=0;i<p.MaxCluster;i++)
      for (j=0;j<p.MaxCluster;j++)
        this.D_cluster[i][j] = 0;

    for (i=0;i<p.n;i++)
    {this.free[i]=1; this.Nearest[i]=Constants.MAX_VALUE; this.Pre_distance[i]=-Constants.MAX_VALUE; this.Nearest_proto[i]=0;}//on initialise avec le cluster 0. 

    winner = 0;
    if (p.Init_Aleatoire == 0)
    {
      for (j=0;j<p.p;j++)
        vector[j] = Constants.MAX_VALUE;

      //on cherche le min...
      for (i=0;i<p.n;i++)
      {
        for (j=0;j<p.p;j++)
          if (vector[j] > data[i][j])
            vector[j] = data[i][j];
      }
      for (i=0;i<p.n;i++)
      {
        dist = p.fdist(vector,data[i],p.p);
        if (dist < d_min)
        {
          d_min = dist;
          winner = i;
        }
      }
      this.Nearest[winner] = 0;
    }
    else
    {
      winner = Math.abs(rand.nextInt())%(p.n-1);
      this.Nearest[winner] = 0;
    }
    this.Weight[0] = p.n;

    this.Nearest_first[0] = 0;
    this.Pre_individu[0] = winner;
    this.free[winner] = 0;
    this.Liste_selectionne[0] = winner;
    //this.Order_selection[winner] = 0;

    for (i=0;i<p.n;i++)//pour tous les �l�ments de T.
      this.Nearest_proto[i] = 0;

    this.C_stable = 0;
    this.Ref_distance = -1;
    this.Ref_OK = 0;
    this.Nb_isole = 0;
    this.Nb_bruit = 0;

    return winner;
      
  }

  void InitBoucle(int N_instanceOld,int New_instance)
  {
    int i;

    this.Liste_selectionne[N_instanceOld] = New_instance;

    if (N_instanceOld > 0)//On disable certains prototyppes au sens de l'in�galit� triangulaire...
      Utils.DisablePrototype(this.Pre_distance,this.D_cluster[N_instanceOld],N_instanceOld+1,this.disable);

    for (i=0;i<N_instanceOld+1;i++)//on initialise la distance pour l'ensemble DS.
    {	this.Pre_distance[i] = 0; this.Pre_individu[i] = -1; }

  }

  int FindNewInstanceMassique(double [][]data, ParamFCNNrule P, int N_instance)
  {
    int i,winner;
    double dist, DistWinner = 0;
    int NewInstance;
    int Data_NewInstance;
    int maxWeight,winWeight;
//double Min,Max,Moy,Ecart;
    int OK_fin=0;
    double maxmax = 0;
    double RatioW;

    NewInstance = N_instance - 1;
    Data_NewInstance = this.Liste_selectionne[NewInstance];
    this.Nearest_proto[Data_NewInstance] = NewInstance;

    if (N_instance > 1 && this.WorseWinner!=-1)
    {
      this.Weight[N_instance-1] = 1;//il y a donc 1 individu dans la nouvelle instance
      this.Weight[this.WorseWinner] = this.Weight[this.WorseWinner] - 1; 	// 1 de moins dans l'ancienne.
    }

    this.WorseWinner = -1;

    for (i=0;i<P.n;i++)//pour tous les �l�ments de T.
    {
      if (this.free[i] == 1)//s'il est libre.
      {
        winner = this.Nearest_proto[i];//l'instance la plus proche est m�moris�...on garde en m�moire l'ancien.
        if (this.disable[this.Nearest_proto[i]] == 0)//On ne peut pas sur la base du prototype se d�cider.
        {
          if (Utils.InegalTriangular(this.D_cluster,this.Nearest[i],this.Nearest_proto[i],NewInstance)>0)
          {
            dist = P.fdist(data[i],data[Data_NewInstance],P.p);
            if (dist < this.Nearest[i])//plus proche d'un prototype...
            {
              this.Nearest[i] = dist;//on update le Farest.					
              this.Weight[winner] = this.Weight[winner] - 1;//on diminue de 1 l'instance impact�e.
              this.Weight[NewInstance] = this.Weight[NewInstance] + 1; //on augmente de 1 l'instance impact�e.
              this.Nearest_proto[i] = NewInstance;//on update le prototype...
              winner = NewInstance;//le prototype gagnant.	
            }
          }//fin du for if.
        }//on regarde son plus proche.

        if (this.Nearest[i] >= this.Pre_distance[winner])//le plus �loign� du plus proche
        {
          this.Pre_distance[winner] = this.Nearest[i];//la distance entre le prototype et son plus �loign�.
          this.Pre_individu[winner] = i;// i devient le plus eloign� des plus proches!		
        }//fin du if nearest[i].
      }//fin du if free.
    }//fin du for i

    //on calcule le max max.
    for (i=0;i<N_instance;i++)
      if (this.Pre_distance[i] > maxmax)
      {
        maxmax = this.Pre_distance[i];
        winner = i;
      }

    if (this.Mass_Weight_Criterion==1)
    {
      winWeight = maxWeight = 0;
      OK_fin = 0;
      for (i=0;i<N_instance;i++)
      {
        //On conditionne avec les distances.
        if (N_instance >= 2)
        {
          if (this.Weight[i] > maxWeight)//recherche du MAX
          {
            RatioW =  1 + P.TradeoffDensite*(double)(this.Weight[i])/(double)(P.NbPointsMin);
            if  (this.Pre_distance[i]*RatioW>=P.dav_1knn[N_instance - 2])//Moyenne � l'n-2. 
            {
              maxWeight = this.Weight[i];//le poids maximum.
              winWeight = i;//le num�ro du prototype gagnant
              OK_fin = 1;
            }
          }
        }
        else
        {
          if (this.Weight[i] > maxWeight)//aucune condition sur les distances.
          {
            maxWeight = this.Weight[i];//le poids maximum.
            winWeight = i;//le num�ro du prototype gagnant
            OK_fin = 1;
          }
        }
      }

      if (maxWeight <= P.NbPointsMin || OK_fin==0)
      {
        this.Ref_distance = P.FactorDistance * P.dav_1knn[N_instance - 2];

        this.Mass_Weight_Criterion = 0;
      }

      if (OK_fin == 0)//il n'y en a plus...
        return -2;

      NewInstance = this.Liste_selectionne[winWeight];//on va chercher l'individu le plus �loign�.
      this.WorseWinner = winWeight;//numero de l'instance.
    }
    else
    {
      DistWinner = 0;
      for (i=0;i<N_instance;i++)
      {
        if (this.Pre_distance[i] > DistWinner)//le prototype ayant le plus �loign� des individus.
        {
          this.WorseWinner = i;//le nouveau prototype.
          DistWinner = this.Pre_distance[this.WorseWinner];
        }
      }
      winWeight = this.WorseWinner;
    }

    if (this.WorseWinner == -1)
      return -1;
    else
      return this.Pre_individu[winWeight];
  }

  int CalculCost(ParamFCNNrule P,int N_instance)
  {
    int i;
    double  CostC;
    double Dist;

    CostC = 0;
    Dist = 0;
    for (i=0;i<N_instance;i++)
    {
      CostC = CostC + this.Weight[i]*(this.Pre_distance[i]);
      Dist += this.Pre_distance[i];
    }


    CostC /= (double)(P.n*this.Max_distance);
    P.Cost_n[N_instance - 1] = CostC;

    if (CostC<P.Granularity)
      return 1;
    else
      return 0;
  }

  void ProcessCriteria(int N_instance, ParamFCNNrule P)
  {
    int i;
    double dmin, dmax;
    double M;

    if (P.Auto == 0)
      return;

    dmin = 100000;
    dmax = 0;
    M=0;
    for (i=0;i<N_instance;i++)
    {
      if (this.Pre_distance[i] < dmin)
        if (this.Weight[i] > P.NbPointsNoise)
          dmin = this.Pre_distance[i];

      if (this.Pre_distance[i] > dmax)
        dmax = this.Pre_distance[i];

      M+=this.Pre_distance[i];
    }

    P.dav_1knn[N_instance - 1] = M/(double)(N_instance);
    P.d_1knn[N_instance - 1] = dmax;
    P.d_1knnmin[N_instance - 1] = dmin;

  }

  void UpdateAlgorithm(int N_instance,int Data_Newinstance)
  {
    if (Data_Newinstance != -1)
    {
      this.Nearest[Data_Newinstance] = 0;
      this.free[Data_Newinstance] = 0;//on l'enl�ve de la liste.
      this.Liste_selectionne[N_instance] = Data_Newinstance;//on met dans la liste.
    }
  }

  int TestFin_DENDIS(ParamFCNNrule P, Integer N_instance)
  {

    if (N_instance == P.n-1)
      return 1;

    if (N_instance >= P.MaxCluster)
      return 1;

    if (P.Auto == 1)//on utilise la distance...
    {
      if (this.Mass_Weight_Criterion == 0 && P.SansStep2 == 1)
        return 1;

      if (P.d_1knn[N_instance - 1] < this.Ref_distance)
        return 1;
    }

    return 0;
  }

  void Update1knnPrototypeMasse(double [][]data, ParamFCNNrule P, int N_instance)
  {
    int i;
    Double Moy,Ecart,Min,Max;
    int []booleen;

    Moy = 0.;
    Ecart = 0.;
    Min = 0.;
    Max = 0.;
    //Update des distances entre clusters.
    P.dav_1knn[N_instance - 1] = 0;

    booleen = new int[N_instance];
    for (i=0;i<N_instance;i++)
    {
      if (this.Weight[i] > P.Granularity*0.1*P.n)
        booleen[i] = 1;
      else
        booleen[i] = 0;
      Utils.CalculusStatVectorBool(this.Pre_distance,N_instance,booleen,Moy,Ecart,Min,Max);
    }

    for (i=0;i<N_instance;i++)
    {
      this.D_cluster[i][N_instance] = P.fdist(data[this.Liste_selectionne[i]],data[this.Pre_individu[this.WorseWinner]],P.p);
      this.D_cluster[N_instance][i] = this.D_cluster[i][N_instance];
//		P.dav_1knn[N_instance - 1] += this.Pre_distance[i]; 
    }
//	P.dav_1knn[N_instance - 1] /= (double)(N_instance);
    P.dav_1knn[N_instance - 1] = Moy;//la moyenne d�nu�e du bruit.


    //Calcul des 1 plus proches voisins entre prototype.
    if (P.Auto == 1 || P.Save==1)
    {
      if (N_instance == 1)//donc 2...avec le nouveau!
        this.Ave_d1nn_proto = this.Pre_distance[this.WorseWinner];
      else
        this.Ave_d1nn_proto = (this.Ave_d1nn_proto*(double)(N_instance) - this.d1nn_proto[this.WorseWinner] + 2*this.Pre_distance[this.WorseWinner]) / (double)(N_instance + 1);

      this.d1nn_proto[this.WorseWinner] = this.Pre_distance[this.WorseWinner];//le nouveau prototype est forc�ment plus proche de son prototype d'origine: les 2 distances min sont modifi�es.
      this.d1nn_proto[N_instance] = this.Pre_distance[this.WorseWinner];
      //P.Ave_d1nn_proto[N_instance] = this.Ave_d1nn_proto;
    }

  }

  void AttributionPattern(int N_instance,int [][]Liste_pattern, ParamFCNNrule P)
  {
    int i,j;
    int []w_int;
    int winner;

    if (P.L_pattern == 0)
      return;

    w_int = new int[P.n];

    for (i=0;i<P.n;i++)
      w_int[i] = 0;

    for (j=0;j<P.n;j++)
    {
      winner = this.Nearest_proto[j];
      w_int[winner]++;
    }

    for (i=0;i<N_instance;i++)//On a un pattern non attribu� � ce niveau.
    {
      if (Liste_pattern[i]!=null)
      {
        Liste_pattern[i] = null;
      }

      if (w_int[i] > 0)
        Liste_pattern[i] = new int[w_int[i]];
      w_int[i] = 0;
    }

    for (j=0;j<P.n;j++)
    {
      winner = this.Nearest_proto[j];
      Liste_pattern[winner][w_int[winner]] = j;
      w_int[winner]++;
    }
  }


}